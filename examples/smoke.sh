#!/usr/bin/env bash
# Smoke test for the examples sandbox: drives service-a over HTTP and checks the hub topology.
# Usage: bash examples/smoke.sh      (A=http://localhost:8091 HUB=http://localhost:7001 to override;
#        SKIP_PAUSE=1 skips the docker-pause step, ONLY_PAUSE=1 runs only it, B_CONTAINER names service-b)
set -euo pipefail

A=${A:-http://localhost:8091}
HUB=${HUB:-http://localhost:7001}

fail() { echo "SMOKE FAILED: $*" >&2; exit 1; }

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

echo "SMOKE OK"
