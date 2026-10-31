#!/usr/bin/env bash
# Smoke test for the examples sandbox: drives service-a over HTTP and checks the hub topology.
# Usage: bash examples/smoke.sh      (A=http://localhost:8091 HUB=http://localhost:7001 to override;
#        SKIP_PAUSE=1 skips the docker-pause step, ONLY_PAUSE=1 runs only it, B_CONTAINER names service-b;
#        SKIP_FLOW=1 skips the flow-control steps, ONLY_FLOW=1 runs only them; INFRA_DIR names kraken/infra)
set -euo pipefail

A=${A:-http://localhost:8091}
HUB=${HUB:-http://localhost:7001}

fail() { echo "SMOKE FAILED: $*" >&2; exit 1; }

# F3 (P1): flow control. The steps recreate service-b and service-c with kraken/infra/compose.flow-demo.yaml (one
# permit each, a 200 ms CountGreetings), so parallel calls queue in the hub and come back as latency, not timeouts
# or 503s, and revert them to the default concurrency afterwards (also when a step fails). Needs the docker CLI.
# SKIP_FLOW=1 skips these steps; ONLY_FLOW=1 runs only them. Timings in ms need perl (Time::HiRes).
INFRA=${INFRA_DIR:-$(cd "$(dirname "$0")/../../kraken/infra" 2>/dev/null && pwd || true)}

# Recreates service-b and service-c from the sandbox compose files plus the given extra files, without touching
# their dependencies, and waits until both are registered with permits matching <pattern>.
recreate_b_and_c() { # <permits pattern> [extra compose file ...]
  local pattern=$1 i topo b c
  shift
  local files=(-f compose.yaml -f compose.local.yaml)
  for f in "$@"; do files+=(-f "$f"); done
  (cd "$INFRA" && docker compose "${files[@]}" --profile examples up -d --no-deps --no-build service-b service-c) \
    >/dev/null 2>&1 || fail "docker compose could not recreate service-b and service-c in $INFRA"
  for i in $(seq 1 120); do
    topo=$(curl -fsS "$HUB/topology" 2>/dev/null || true)
    b=$(instance_fields "$topo" service-b)
    c=$(instance_fields "$topo" service-c)
    if echo "$b" | grep -Eq "\"permitsAvailable\":$pattern," && echo "$c" | grep -Eq "\"permitsAvailable\":$pattern," \
       && echo "$topo" | grep -q '"type":"examples.CountGreetings","kind":"QUERY","ownerService":"service-b","liveInstances":1'; then
      return 0
    fi
    sleep 1
  done
  fail "service-b and service-c did not come back with permits $pattern in 120s; service-b: $b; service-c: $c"
}

flow_demo_on() {
  command -v docker >/dev/null || fail "docker CLI needed for the flow steps (or set SKIP_FLOW=1)"
  [ -n "$INFRA" ] && [ -f "$INFRA/compose.flow-demo.yaml" ] || fail "kraken/infra not found (set INFRA_DIR)"
  echo "11b. recreating service-b and service-c with compose.flow-demo.yaml (one permit each, 200 ms counts) ..."
  trap flow_demo_off EXIT
  recreate_b_and_c 1 compose.flow-demo.yaml
}

flow_demo_off() {
  trap - EXIT
  echo "14. reverting service-b and service-c to the default concurrency ..."
  recreate_b_and_c '([2-9]|[1-9][0-9]+)'
}
parallel_calls() { # <n> <method> <url> [body]: prints one HTTP code per line
  local n=$1 method=$2 url=$3 body=${4:-} dir
  dir=$(mktemp -d)
  for i in $(seq 1 "$n"); do
    if [ -n "$body" ]; then
      curl -s -o /dev/null -w '%{http_code}\n' --max-time 30 -X "$method" "$url" -H 'content-type: application/json' -d "$body" > "$dir/$i" &
    else
      curl -s -o /dev/null -w '%{http_code}\n' --max-time 30 -X "$method" "$url" > "$dir/$i" &
    fi
  done
  wait
  cat "$dir"/*
  rm -rf "$dir"
}

now_ms() { perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'; }

# The fields of <service>'s first instance on the hub topology, on one line ("instanceId": ... up to "tags":).
# Splitting at '{' keeps it to grep and tr: the type arrays before "flowControlled" rule out a single regex.
# Empty (never a failure, which pipefail and set -e would turn into a silent exit) while the service is not listed.
instance_fields() { # <topology json> <service>
  printf '%s' "$1" | tr '{' '\n' | { grep -A1 "^\"serviceName\":\"$2\"" || true; } | tail -n 1
}

# After a burst the permits must come back (a lost permit would stall the instance): flow-controlled, permits
# available (the sandbox's one), nothing queued.
expect_permit_back() { # <service>
  local i fields
  for i in $(seq 1 20); do
    fields=$(instance_fields "$(curl -fsS "$HUB/topology" 2>/dev/null || true)" "$1")
    if echo "$fields" | grep -q '"flowControlled":true,"permitsAvailable":[1-9][0-9]*,"queued":0'; then return 0; fi
    sleep 0.1
  done
  fail "$1 is not flow-controlled with its permit back on the hub: $fields"
}

flow_steps() {
  flow_demo_on
  echo "12. 20 parallel CountGreetings calls into service-b (1 permit, 200 ms each): all succeed, queued in the hub ..."
  local started elapsed codes ok out pid queued max_queued=0
  out=$(mktemp)
  started=$(now_ms)
  parallel_calls 20 GET "$A/greetings/Ada/count" > "$out" &
  pid=$!
  while kill -0 "$pid" 2>/dev/null; do
    queued=$(instance_fields "$(curl -fsS "$HUB/topology" 2>/dev/null || true)" service-b \
      | sed -n 's/.*"queued":\([0-9]*\).*/\1/p')
    if [ "${queued:-0}" -gt "$max_queued" ]; then max_queued=$queued; fi
    sleep 0.1
  done
  wait "$pid" || true
  elapsed=$(( $(now_ms) - started ))
  codes=$(cat "$out")
  rm -f "$out"
  ok=$(echo "$codes" | grep -c '^200$' || true)
  echo "   $ok of 20 answered 200 in ${elapsed} ms (one at a time: at least 20 x 200 ms); up to $max_queued queued in the hub"
  [ "$ok" -eq 20 ] || fail "expected 20 x 200, got: $(echo "$codes" | sort | uniq -c | tr '\n' ' ')"
  [ "$elapsed" -ge 4000 ] || fail "20 calls of 200 ms finished in ${elapsed} ms: not serialised by the permit"
  [ "$max_queued" -ge 1 ] || fail "the hub never showed a request queued for service-b"
  expect_permit_back service-b

  echo "13. cross-language under flow control: 10 parallel Shouts from Kotlin service-a to Rust service-c (1 permit) ..."
  started=$(now_ms)
  codes=$(parallel_calls 10 POST "$A/shouts" '{"text":"flow"}')
  elapsed=$(( $(now_ms) - started ))
  ok=$(echo "$codes" | grep -c '^200$' || true)
  echo "   $ok of 10 answered 200 in ${elapsed} ms"
  [ "$ok" -eq 10 ] || fail "expected 10 x 200, got: $(echo "$codes" | sort | uniq -c | tr '\n' ' ')"
  expect_permit_back service-c
  flow_demo_off
}

# F1 (P0): a frozen handler is evicted after the 10 s v2 heartbeat timeout, so a call to it fails in ~10 s
# instead of the 30 s it took before (and instead of burning a 5 s TIMEOUT: compose.local.yaml gives
# examples.CountGreetings a 20 s per-type override). SKIP_PAUSE=1 skips this step; ONLY_PAUSE=1 runs only this step.
pause_step() {
  local b=${B_CONTAINER:-kraken-example-b}
  command -v docker >/dev/null || fail "docker CLI needed for the pause step (or set SKIP_PAUSE=1)"
  docker inspect "$b" >/dev/null 2>&1 || fail "container $b not found (set B_CONTAINER)"
  echo "10. pausing $b during a CountGreetings call: expect HANDLER_DISCONNECTED (503) in about 10 s ..."
  local body
  body=$(mktemp)
  docker pause "$b" >/dev/null
  trap 'docker unpause '"$b"' >/dev/null 2>&1 || true' EXIT
  local started ended code
  started=$(date +%s)
  code=$(curl -s -o "$body" -w '%{http_code}' --max-time 45 "$A/greetings/Ada/count" || true)
  ended=$(date +%s)
  docker unpause "$b" >/dev/null
  trap - EXIT
  local elapsed=$((ended - started))
  echo "   HTTP $code after ${elapsed}s: $(cat "$body")"
  [ "$code" = "503" ] || fail "expected 503 (HandlerDisconnectedException), got $code after ${elapsed}s: $(cat "$body")"
  grep -q 'HandlerDisconnectedException' "$body" || fail "expected HandlerDisconnectedException, got: $(cat "$body")"
  [ "$elapsed" -ge 5 ] && [ "$elapsed" -le 16 ] || fail "expected about 10 s (5..16), took ${elapsed}s"
  rm -f "$body"

  echo "11. waiting for service-b to come back on the hub and answer again ..."
  for i in $(seq 1 60); do
    if curl -fsS "$A/greetings/Ada/count" >/dev/null 2>&1; then break; fi
    [ "$i" -eq 60 ] && fail "service-b did not come back within 60s after unpause"
    sleep 1
  done
  echo "   service-b answers again after ${i}s"
}

echo "1. waiting for service-a at $A ..."
for i in $(seq 1 90); do
  if curl -fsS "$A/actuator/health" 2>/dev/null | grep -q '"UP"'; then break; fi
  [ "$i" -eq 90 ] && fail "service-a did not become healthy in 90s"
  sleep 1
done

echo "1b. waiting for service-b to own examples.CountGreetings on the hub ($HUB) ..."
for i in $(seq 1 90); do
  TOPO=$(curl -fsS "$HUB/topology" 2>/dev/null || true)
  if echo "$TOPO" | grep -q '"type":"examples.CountGreetings","kind":"QUERY","ownerService":"service-b","liveInstances":[1-9]'; then break; fi
  [ "$i" -eq 90 ] && fail "service-b did not register on the hub in 90s; topology: $TOPO"
  sleep 1
done

if [ "${ONLY_FLOW:-0}" = "1" ]; then
  flow_steps
  echo "SMOKE OK (flow steps only)"
  exit 0
fi

if [ "${ONLY_PAUSE:-0}" = "1" ]; then
  pause_step
  echo "SMOKE OK (pause step only)"
  exit 0
fi

post_greeting() {
  local body
  body=$(curl -fsS -X POST "$A/greetings" -H 'content-type: application/json' -d '{"name":"Ada"}') \
    || fail "POST /greetings failed"
  echo "$body" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p'
}

echo "2. reading the current count for Ada, then sending two greetings ..."
BEFORE=$(curl -fsS "$A/greetings/Ada/count" | sed -n 's/.*"count":\([0-9]*\).*/\1/p') || fail "GET /greetings/Ada/count failed"
[ -n "$BEFORE" ] || fail "no count in the query reply"
ID1=$(post_greeting); ID2=$(post_greeting)
[ -n "$ID1" ] && [ -n "$ID2" ] || fail "no id in the command reply (got '$ID1' / '$ID2')"
echo "   accepted ids: $ID1 $ID2"

echo "3. waiting for both GreetingDelivered events to reach service-a (a cold Kotlin consumer can take ~45 s to join its group) ..."
for i in $(seq 1 90); do
  DELIVERED=$(curl -fsS "$A/greetings/delivered")
  if echo "$DELIVERED" | grep -q "$ID1" && echo "$DELIVERED" | grep -q "$ID2"; then break; fi
  [ "$i" -eq 90 ] && fail "events not delivered within 90s; last body: $DELIVERED"
  sleep 1
done
echo "   delivered: $DELIVERED"

echo "4. querying the count (expect $BEFORE + 2; service-b counts in memory, so it is 2 on a fresh B) ..."
COUNT=$(curl -fsS "$A/greetings/Ada/count") || fail "GET /greetings/Ada/count failed"
echo "$COUNT" | grep -q "\"count\":$((BEFORE + 2))" || fail "expected \"count\":$((BEFORE + 2)), got: $COUNT"
echo "   $COUNT"

echo "5. hub topology ..."
TOPOLOGY=$(curl -fsS "$HUB/topology") || fail "GET $HUB/topology failed"
echo "$TOPOLOGY"
echo "$TOPOLOGY" | grep -q '"service-a"' || fail "service-a missing from topology"
echo "$TOPOLOGY" | grep -q '"service-b"' || fail "service-b missing from topology"

echo "6. waiting for service-c to own examples.Shout and examples.Reverse on the hub ..."
for i in $(seq 1 90); do
  TOPO=$(curl -fsS "$HUB/topology" 2>/dev/null || true)
  if echo "$TOPO" | grep -q '"type":"examples.Reverse","kind":"QUERY","ownerService":"service-c","liveInstances":[1-9]' \
     && echo "$TOPO" | grep -q '"type":"examples.Shout","kind":"COMMAND","ownerService":"service-c","liveInstances":[1-9]'; then break; fi
  [ "$i" -eq 90 ] && fail "service-c did not register on the hub in 90s; topology: $TOPO"
  sleep 1
done

# A unique text per run: service-a keeps delivered events in memory, so a fixed text would pass on a previous run's data.
T="abc$(date +%s)"
UP=$(printf '%s' "$T" | tr a-z A-Z)
REV=$(printf '%s' "$T" | rev)
echo "7. shouting '$T' through service-c ..."
SHOUTED=$(curl -fsS -X POST "$A/shouts" -H 'content-type: application/json' -d "{\"text\":\"$T\"}") || fail "POST /shouts failed"
echo "$SHOUTED" | grep -q "\"text\":\"$UP\"" || fail "expected \"text\":\"$UP\", got: $SHOUTED"
echo "   $SHOUTED"

echo "8. reversing '$T' through service-c ..."
REVERSED=$(curl -fsS "$A/reverse/$T") || fail "GET /reverse/$T failed"
echo "$REVERSED" | grep -q "\"text\":\"$REV\"" || fail "expected \"text\":\"$REV\", got: $REVERSED"
echo "   $REVERSED"

echo "9. waiting for TextShouted and TextReversed to reach service-a ..."
for i in $(seq 1 90); do
  TEXTS=$(curl -fsS "$A/texts/delivered" || true)
  if echo "$TEXTS" | grep -q "\"kind\":\"shouted\",\"text\":\"$UP\"" && echo "$TEXTS" | grep -q "\"kind\":\"reversed\",\"text\":\"$REV\""; then break; fi
  [ "$i" -eq 90 ] && fail "text events not delivered within 90s; last body: $TEXTS"
  sleep 1
done
echo "   delivered: $TEXTS"

[ "${SKIP_PAUSE:-0}" = "1" ] || pause_step
[ "${SKIP_FLOW:-0}" = "1" ] || flow_steps

echo "SMOKE OK"
