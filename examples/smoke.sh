#!/usr/bin/env bash
# Smoke test for the examples sandbox: drives service-a over HTTP and checks the hub topology.
# Usage: bash examples/smoke.sh      (A=http://localhost:8091 HUB=http://localhost:7001 HUB2=http://localhost:7011 to
#        override; SKIP_PAUSE=1 skips the docker-pause step, ONLY_PAUSE=1 runs only it, B_CONTAINER names service-b;
#        SKIP_FLOW=1 skips the flow-control steps, ONLY_FLOW=1 runs only them; INFRA_DIR names kraken/infra;
#        SKIP_HA=1 skips the two-hub steps, ONLY_HA=1 runs only them, HUB2_CONTAINER names the second hub;
#        SKIP_SCATTER=1 skips the scatter-gather steps, ONLY_SCATTER=1 runs only them)
set -euo pipefail

A=${A:-http://localhost:8091}
HUB=${HUB:-http://localhost:7001}
HUB2=${HUB2:-http://localhost:7011}

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

# P3 (ADR 0016): every example instance connects to both hubs. JSON is read with jq, else python3 (field order and
# whitespace do not matter); SKIP_HA=1 skips these steps, ONLY_HA=1 runs only them.
if command -v jq >/dev/null; then
  hub_id_of() { jq -r '.hubId // empty' 2>/dev/null || true; }
  # One line per instance of service-a, service-b and service-c: "<service> <instanceId> <connectedHubIds, comma-separated>".
  example_instances() {
    jq -r '.services[] | select(.serviceName | test("^service-[abc]$")) | .serviceName as $s | .instances[]
           | "\($s) \(.instanceId) \((.connectedHubIds // []) | join(","))"' 2>/dev/null || true
  }
  peers_of() { jq -r '(.peers // [])[]' 2>/dev/null || true; }
  # /api/v1/types -> "<shared> <owners, comma-separated>" of examples.Inventory; empty while it is not listed.
  inventory_type() {
    jq -r '.types[] | select(.type == "examples.Inventory") | "\(.shared) \(.owners | join(","))"' 2>/dev/null || true
  }
  # /scatter/inventory -> "<parts> <failed> <timedOut> <answering services, sorted, comma-separated>".
  scatter_summary() {
    jq -r '"\(.parts | length) \(.failed) \(.timedOut) \([.parts[].service] | unique | join(","))"' 2>/dev/null || true
  }
  service_of() { jq -r '.service // empty' 2>/dev/null || true; }
elif command -v python3 >/dev/null; then
  hub_id_of() { python3 -I -c 'import json,sys
try: print(json.load(sys.stdin).get("hubId") or "")
except Exception: pass' || true; }
  example_instances() { python3 -I -c 'import json,re,sys
try: t = json.load(sys.stdin)
except Exception: sys.exit(0)
for s in t.get("services", []):
    if re.fullmatch(r"service-[abc]", s.get("serviceName", "")):
        for i in s.get("instances", []):
            print(s["serviceName"], i["instanceId"], ",".join(i.get("connectedHubIds") or []))' || true; }
  peers_of() { python3 -I -c 'import json,sys
try: print("\n".join(json.load(sys.stdin).get("peers") or []))
except Exception: pass' || true; }
  inventory_type() { python3 -I -c 'import json,sys
try:
    for t in json.load(sys.stdin).get("types", []):
        if t.get("type") == "examples.Inventory":
            print(str(bool(t.get("shared"))).lower(), ",".join(t.get("owners") or []))
except Exception: pass' || true; }
  scatter_summary() { python3 -I -c 'import json,sys
try:
    g = json.load(sys.stdin); parts = g.get("parts") or []
    print(len(parts), g.get("failed"), g.get("timedOut"), ",".join(sorted({p.get("service", "") for p in parts})))
except Exception: pass' || true; }
  service_of() { python3 -I -c 'import json,sys
try: print(json.load(sys.stdin).get("service") or "")
except Exception: pass' || true; }
else
  hub_id_of() { fail "jq or python3 needed for the two-hub steps (or set SKIP_HA=1)"; }
  example_instances() { hub_id_of; }
  peers_of() { hub_id_of; }
  inventory_type() { fail "jq or python3 needed for the scatter-gather steps (or set SKIP_SCATTER=1)"; }
  scatter_summary() { inventory_type; }
  service_of() { inventory_type; }
fi

# Succeeds (prints nothing) when both hubs list the same instances of service-a, service-b and service-c, each of
# them at least once, and every instance reports both hub ids as connected; else prints why.
both_hubs_problem() {
  local t1 t2 id1 id2 l1 l2 svc
  t1=$(curl -fsS --max-time 3 "$HUB/topology" 2>/dev/null || true)
  t2=$(curl -fsS --max-time 3 "$HUB2/topology" 2>/dev/null || true)
  id1=$(printf '%s' "$t1" | hub_id_of); id2=$(printf '%s' "$t2" | hub_id_of)
  [ -n "$id1" ] || { echo "$HUB/topology unreachable"; return; }
  [ -n "$id2" ] || { echo "$HUB2/topology unreachable"; return; }
  l1=$(printf '%s' "$t1" | example_instances | sort); l2=$(printf '%s' "$t2" | example_instances | sort)
  for svc in service-a service-b service-c; do
    echo "$l1" | grep -q "^$svc " || { echo "$svc missing on $id1"; return; }
    echo "$l2" | grep -q "^$svc " || { echo "$svc missing on $id2"; return; }
  done
  [ "$(echo "$l1" | cut -d' ' -f1,2)" = "$(echo "$l2" | cut -d' ' -f1,2)" ] \
    || { echo "the hubs list different instances: $id1: $(echo "$l1" | cut -d' ' -f1,2 | tr '\n' ' ')/ $id2: $(echo "$l2" | cut -d' ' -f1,2 | tr '\n' ' ')"; return; }
  printf '%s\n%s\n' "$l1" "$l2" | while read -r svc iid hubs; do
    case ",$hubs," in *",$id1,"*) ;; *) echo "$svc $iid reports [$hubs], not $id1"; break ;; esac
    case ",$hubs," in *",$id2,"*) ;; *) echo "$svc $iid reports [$hubs], not $id2"; break ;; esac
  done
}

await_both_hubs() { # <seconds>: prints the seconds it took; fails after <seconds>
  local started why
  started=$(now_ms)
  while :; do
    why=$(both_hubs_problem)
    [ -z "$why" ] && { echo $(( ($(now_ms) - started) / 1000 )); return 0; }
    [ $(( $(now_ms) - started )) -ge $(( $1 * 1000 )) ] && fail "not every example instance is on both hubs after $1 s: $why"
    sleep 0.5
  done
}

# One call through service-a per line of <out>: "<http code> <ms> <started at, ms since <t0>>", alternating a Kotlin
# (CountGreetings, service-b) and a Rust (Reverse, service-c) target, until <stop file> exists.
ha_loop() { # <out> <stop file> <t0>
  local n=0 started code url
  while [ ! -e "$2" ]; do
    n=$((n + 1))
    if [ $((n % 2)) -eq 0 ]; then url="$A/reverse/ha$n"; else url="$A/greetings/Ada/count"; fi
    started=$(now_ms)
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$url" || true)
    echo "$code $(( $(now_ms) - started )) $(( started - $3 ))" >> "$1"
  done
}

# permitsAvailable of <service>'s (first) instance on <hub>, empty when it is not listed.
permits_on() { # <hub url> <service>
  instance_fields "$(curl -fsS "$1/topology" 2>/dev/null || true)" "$2" | sed -n 's/.*"permitsAvailable":\([0-9]*\).*/\1/p'
}

ha_steps() {
  local h2=${HUB2_CONTAINER:-kraken-hub-2} secs peers p out stop t0 pid stopped_at gone_at total failed max late_failed after
  local share permits topped_at
  command -v docker >/dev/null || fail "docker CLI needed for the two-hub steps (or set SKIP_HA=1)"
  docker inspect "$h2" >/dev/null 2>&1 || fail "container $h2 not found (set HUB2_CONTAINER, or SKIP_HA=1)"

  echo "15. both hubs ($HUB, $HUB2) list every example instance, each reporting both hub ids ..."
  secs=$(await_both_hubs 15)
  echo "   on both hubs after ${secs}s: $(curl -fsS "$HUB/topology" | example_instances | cut -d' ' -f1,3 | sort | uniq -c | tr -s ' ' | tr '\n' ';')"

  echo "16. $HUB2/api/v1/version lists both dashboards as peers ..."
  peers=$(curl -fsS "$HUB2/api/v1/version" | peers_of) || fail "GET $HUB2/api/v1/version failed"
  for p in "$HUB" "$HUB2"; do
    echo "$peers" | grep -qx "${p%/}" || fail "peer $p missing from $HUB2/api/v1/version: $(echo "$peers" | tr '\n' ' ')"
  done
  echo "   peers: $(echo "$peers" | tr '\n' ' ')"

  echo "17. calls through service-a keep answering while $h2 is stopped, and every instance is back on it after a start ..."
  # service-b (Kotlin) splits its handlers over both hubs; once hub-2 is gone, hub-1 must hold (about) twice its share:
  # the client's top-up carries Permits.grant, which raises hub-1's cap on the session.
  share=$(permits_on "$HUB" service-b)
  # Permits are never revoked: after an earlier failover (a previous run of this step) $HUB still holds service-b's
  # whole count, more than its share on $HUB2, and there is nothing left to top up. Recreate service-b so the step
  # starts from the split again.
  p=$(permits_on "$HUB2" service-b)
  if [ -n "$share" ] && [ -n "$p" ] && [ "$share" -gt $(( p + 2 )) ]; then
    echo "   service-b holds $share permits on $HUB but $p on $HUB2 (topped up by an earlier failover); recreating it ..."
    [ -n "$INFRA" ] && [ -f "$INFRA/compose.local.yaml" ] || fail "kraken/infra not found (set INFRA_DIR)"
    (cd "$INFRA" && docker compose -f compose.yaml -f compose.local.yaml --profile examples \
      up -d --no-deps --no-build --force-recreate service-b) >/dev/null 2>&1 || fail "docker compose could not recreate service-b"
    secs=$(await_both_hubs 90)
    share=$(permits_on "$HUB" service-b)
    echo "   service-b back on both hubs after ${secs}s with $share permits on $HUB, $(permits_on "$HUB2" service-b) on $HUB2"
  fi
  [ -n "$share" ] && [ "$share" -ge 2 ] || fail "service-b shows no usable permits on $HUB before the stop: '$share'"
  out=$(mktemp); stop=$(mktemp -u); t0=$(now_ms)
  trap 'touch '"$stop"'; docker start '"$h2"' >/dev/null 2>&1 || true' EXIT
  ha_loop "$out" "$stop" "$t0" &
  pid=$!
  sleep 2
  stopped_at=$(( $(now_ms) - t0 ))
  docker stop "$h2" >/dev/null
  gone_at=$(( $(now_ms) - t0 ))
  echo "   $h2 stopped (docker stop took $((gone_at - stopped_at)) ms); calls continue for 4 s ..."
  topped_at=""
  while [ $(( $(now_ms) - t0 - gone_at )) -lt 3000 ]; do
    permits=$(permits_on "$HUB" service-b)
    if [ -n "$permits" ] && [ "$permits" -ge $(( 2 * share - 2 )) ]; then topped_at=$(( $(now_ms) - t0 - gone_at )); break; fi
    sleep 0.1
  done
  [ -n "$topped_at" ] || fail "service-b still shows ${permits:-no} permits on $HUB 3 s after $h2 stopped (share $share, expected about $(( 2 * share )))"
  echo "   service-b's permits on $HUB: $share before the stop, $permits ${topped_at} ms after it (the top-up was kept)"
  sleep $(( (4000 - topped_at) / 1000 ))
  touch "$stop"; wait "$pid" || true
  total=$(wc -l < "$out" | tr -d ' ')
  failed=$(awk '$1 != "200"' "$out" | wc -l | tr -d ' ')
  max=$(awk 'BEGIN { m = 0 } $2 > m { m = $2 } END { print m }' "$out")
  # Calls started a second after the container was gone must all succeed: the clients noticed the closed streams.
  after=$(awk -v s="$gone_at" '$3 > s + 1000' "$out" | wc -l | tr -d ' ')
  late_failed=$(awk -v s="$gone_at" '$3 > s + 1000 && $1 != "200"' "$out" | wc -l | tr -d ' ')
  echo "   $total calls, $failed failed ($(awk '$1 != "200" { print $1 }' "$out" | sort | uniq -c | tr -s ' ' | tr '\n' ' ')), max ${max} ms; $after calls from 1 s after the stop, $late_failed of them failed"
  rm -f "$out"
  [ "$failed" -le "${HA_MAX_FAILURES:-5}" ] || fail "$failed calls failed while $h2 stopped (at most ${HA_MAX_FAILURES:-5} in flight may)"
  [ "$max" -le 6000 ] || fail "a call took ${max} ms while $h2 stopped (hang; at most 6000 ms)"
  [ "$after" -ge 5 ] && [ "$late_failed" -eq 0 ] || fail "calls did not keep succeeding on the remaining hub ($late_failed of $after failed)"

  t0=$(now_ms)
  docker start "$h2" >/dev/null
  trap - EXIT
  secs=$(await_both_hubs 15)
  echo "   every example instance is back on $h2 (and reports it) $(( $(now_ms) - t0 )) ms after docker start"
}

# P4a (ADR 0017): examples.Inventory is a shared query type owned by service-b (Kotlin) and service-c (Rust). A
# scatter-gather from service-a asks both; a point query goes to one instance of either. Needs jq or python3;
# SKIP_SCATTER=1 skips these steps, ONLY_SCATTER=1 runs only them.
scatter_steps() {
  local hubs=("$HUB") h i types t body summary parts failed timed_out services svc seen=""
  [ "${SKIP_HA:-0}" = "1" ] || hubs+=("$HUB2")
  echo "18. /api/v1/types on ${hubs[*]}: examples.Inventory is shared, owned by service-b and service-c ..."
  for h in "${hubs[@]}"; do
    for i in $(seq 1 60); do
      t=$(curl -fsS "$h/api/v1/types" 2>/dev/null | inventory_type)
      [ "$t" = "true service-b,service-c" ] && break
      [ "$i" -eq 60 ] && fail "examples.Inventory on $h/api/v1/types is '$t', expected 'true service-b,service-c' (shared, two owners)"
      sleep 1
    done
    echo "   $h: shared=${t%% *} owners=${t#* }"
  done

  echo "18b. GET /scatter/inventory: one gather answered by service-b and service-c, none failed or timed out ..."
  body=$(curl -fsS --max-time 10 "$A/scatter/inventory?item=smoke") || fail "GET /scatter/inventory failed: $(curl -s "$A/scatter/inventory?item=smoke")"
  summary=$(printf '%s' "$body" | scatter_summary)
  read -r parts failed timed_out services <<< "$summary"
  echo "   $body"
  echo "   $parts parts from [$services], failed $failed, timedOut $timed_out"
  for svc in service-b service-c; do
    case ",$services," in *",$svc,"*) ;; *) fail "no part from $svc in the gather: $body" ;; esac
  done
  [ "$failed" = "0" ] && [ "$timed_out" = "0" ] || fail "expected failed 0 and timedOut 0: $body"

  echo "18c. GET /inventory six times: the point query on the shared type reaches both owners ..."
  for i in $(seq 1 6); do
    body=$(curl -fsS --max-time 10 "$A/inventory?item=smoke") || fail "GET /inventory failed (call $i)"
    svc=$(printf '%s' "$body" | service_of)
    [ -n "$svc" ] || fail "no service in the reply: $body"
    seen="$seen $svc"
  done
  echo "   answered by:$seen"
  for svc in service-b service-c; do
    case " $seen " in *" $svc "*) ;; *) fail "six point queries never reached $svc (answered by:$seen)" ;; esac
  done
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

if [ "${ONLY_HA:-0}" = "1" ]; then
  ha_steps
  echo "SMOKE OK (two-hub steps only)"
  exit 0
fi

if [ "${ONLY_SCATTER:-0}" = "1" ]; then
  scatter_steps
  echo "SMOKE OK (scatter-gather steps only)"
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

echo "5b. hub dashboard graph: service-a -> examples.CountGreetings (owner service-b) counted ..."
# With two hubs service-a sends unkeyed requests round-robin, so the edge is on either hub's graph (each hub keeps its own).
EDGE='"from":"service:service-a","to":"type:examples.CountGreetings","role":"send","count":[1-9]'
{ curl -fsS "$HUB/api/v1/graph" 2>/dev/null | grep -q "$EDGE"; } || { curl -fsS "$HUB2/api/v1/graph" 2>/dev/null | grep -q "$EDGE"; } \
  || fail "edge service-a -> examples.CountGreetings missing from $HUB/api/v1/graph and $HUB2/api/v1/graph"

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
[ "${SKIP_HA:-0}" = "1" ] || ha_steps
[ "${SKIP_SCATTER:-0}" = "1" ] || scatter_steps

echo "SMOKE OK"
