#!/usr/bin/env bash
# Smoke test for the examples sandbox: drives service-a over HTTP and checks the hub topology.
# Usage: bash examples/smoke.sh      (A=http://localhost:8091 HUB=http://localhost:7001 to override)
set -euo pipefail

A=${A:-http://localhost:8091}
HUB=${HUB:-http://localhost:7001}

fail() { echo "SMOKE FAILED: $*" >&2; exit 1; }

echo "1. waiting for service-a at $A ..."
for i in $(seq 1 90); do
  if curl -fsS "$A/actuator/health" 2>/dev/null | grep -q '"UP"'; then break; fi
  [ "$i" -eq 90 ] && fail "service-a did not become healthy in 90s"
  sleep 1
done

post_greeting() {
  local body
  body=$(curl -fsS -X POST "$A/greetings" -H 'content-type: application/json' -d '{"name":"Ada"}') \
    || fail "POST /greetings failed"
  echo "$body" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p'
}

echo "2. sending two greetings for Ada ..."
ID1=$(post_greeting); ID2=$(post_greeting)
[ -n "$ID1" ] && [ -n "$ID2" ] || fail "no id in the command reply (got '$ID1' / '$ID2')"
echo "   accepted ids: $ID1 $ID2"

echo "3. waiting for both GreetingDelivered events to reach service-a ..."
for i in $(seq 1 30); do
  DELIVERED=$(curl -fsS "$A/greetings/delivered")
  if echo "$DELIVERED" | grep -q "$ID1" && echo "$DELIVERED" | grep -q "$ID2"; then break; fi
  [ "$i" -eq 30 ] && fail "events not delivered within 30s; last body: $DELIVERED"
  sleep 1
done
echo "   delivered: $DELIVERED"

echo "4. querying the count ..."
COUNT=$(curl -fsS "$A/greetings/Ada/count") || fail "GET /greetings/Ada/count failed"
echo "$COUNT" | grep -q '"count":2' || fail "expected \"count\":2, got: $COUNT"
echo "   $COUNT"

echo "5. hub topology ..."
TOPOLOGY=$(curl -fsS "$HUB/topology") || fail "GET $HUB/topology failed"
echo "$TOPOLOGY"
echo "$TOPOLOGY" | grep -q '"service-a"' || fail "service-a missing from topology"
echo "$TOPOLOGY" | grep -q '"service-b"' || fail "service-b missing from topology"

echo "SMOKE OK"
