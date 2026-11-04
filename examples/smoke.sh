#!/usr/bin/env bash
# Smoke test for the examples sandbox: drives service-a over HTTP and checks the hub topology.
# Usage: bash examples/smoke.sh      (A=http://localhost:8091 HUB=http://localhost:7001 HUB2=http://localhost:7011 to
#        override; SKIP_PAUSE=1 skips the docker-pause step, ONLY_PAUSE=1 runs only it, B_CONTAINER names service-b;
#        SKIP_FLOW=1 skips the flow-control steps, ONLY_FLOW=1 runs only them; INFRA_DIR names kraken/infra;
#        SKIP_HA=1 skips the two-hub steps, ONLY_HA=1 runs only them, HUB2_CONTAINER names the second hub;
#        SKIP_SCATTER=1 skips the scatter-gather steps, ONLY_SCATTER=1 runs only them;
#        SKIP_STREAM=1 skips the streaming-query steps, ONLY_STREAM=1 runs only them;
#        SKIP_SUBSCRIBE=1 skips the subscription-query steps, ONLY_SUBSCRIBE=1 runs only them, B=http://localhost:8092 is
#        service-b's HTTP address;
#        SKIP_POSTCARD=1 skips the saga steps 24-24e (the postcard printer), ONLY_POSTCARD=1 runs only them, PRINTER=http://localhost:8093
#        is the fake printer's address;
#        SKIP_BANNER=1 skips the Rust saga steps 25-25e (service-c's banner printer), ONLY_BANNER=1 runs only them,
#        C=http://localhost:8094 is service-c's HTTP address;
#        SKIP_AUDIT=1 skips the named-processor and dead-letter steps 26-26h (the `audit` processor of service-b and
#        service-c), ONLY_AUDIT=1 runs only them;
#        SKIP_CONTROL=1 skips the processor-control steps 27-27e (pause, resume, reset and dead letters of both `audit`
#        processors through the hub's HTTP API with the write token), ONLY_CONTROL=1 runs only them; WRITE_TOKEN_FILE names
#        the write token, default <secrets directory>/hub-control/dashboard-write.token (kraken/infra/scripts/dev-hub-pki.sh,
#        in every mode);
#        SECURE=1 runs against the secure sandbox (kraken/infra/compose.secure.yaml, secrets from
#        kraken/infra/scripts/dev-hub-pki.sh): the hub calls are https with the dev CA and carry the dashboard token, every
#        other step runs unchanged, and the security steps 21 and 21b are added; SKIP_SECURE=1 skips those two, ONLY_SECURE=1
#        runs only them; SECRETS_DIR names the secrets directory, default KRAKEN_SECRETS_DIR or INFRA_DIR/.secrets;
#        JWT=1 (needs SECURE=1) runs against the JWT overlay on top of it (kraken/infra/compose.jwt.yaml: service-b and
#        service-c authenticate with Keycloak client-credentials tokens, service-a keeps its static token), every other
#        step runs unchanged, and the JWT steps 22 and 22b are added; SKIP_JWT=1 skips those two, ONLY_JWT=1 runs only
#        them, C_CONTAINER names service-c's container;
#        MTLS=1 (needs SECURE=1, excludes JWT=1) runs against the mTLS overlay on top of the secure sandbox
#        (kraken/infra/compose.mtls.yaml: service-a and service-c authenticate with a client certificate, service-b keeps
#        its static token), every other step runs unchanged, and the certificate steps 23 and 23b are added (23b rotates
#        service-c's and service-a's certificates on disk with kraken/infra/scripts/dev-hub-pki.sh --rotate while calls
#        flow); SKIP_MTLS=1 skips those two, ONLY_MTLS=1 runs only them, A_CONTAINER names service-a's container)
set -euo pipefail

SECURE=${SECURE:-0}
JWT=${JWT:-0}
MTLS=${MTLS:-0}
A=${A:-http://localhost:8091}
if [ "$SECURE" = "1" ]; then
  HUB=${HUB:-https://localhost:7001}
  HUB2=${HUB2:-https://localhost:7011}
else
  HUB=${HUB:-http://localhost:7001}
  HUB2=${HUB2:-http://localhost:7011}
fi
B=${B:-http://localhost:8092}

fail() { echo "SMOKE FAILED: $*" >&2; exit 1; }

# F3 (P1): flow control. The steps recreate service-b and service-c with kraken/infra/compose.flow-demo.yaml (one
# permit each, a 200 ms CountGreetings), so parallel calls queue in the hub and come back as latency, not timeouts
# or 503s, and revert them to the default concurrency afterwards (also when a step fails). Needs the docker CLI.
# SKIP_FLOW=1 skips these steps; ONLY_FLOW=1 runs only them. Timings in ms need perl (Time::HiRes).
INFRA=${INFRA_DIR:-$(cd "$(dirname "$0")/../../kraken/infra" 2>/dev/null && pwd || true)}
COMPOSE_FILES=(-f compose.yaml -f compose.local.yaml)

# SECURE=1: the hubs speak TLS with the dev CA and want the dashboard token on /api/v1/*, and the
# sandbox is the compose.secure.yaml overlay. Every step below keeps calling plain `curl "$HUB/..."`: this wrapper adds the
# CA and the token (through a curl config on a pipe, so the token never shows in a process list) to calls aimed at a hub
# and leaves every other call (service-a, service-b) alone.
if [ "$SECURE" = "1" ]; then
  COMPOSE_FILES+=(-f compose.secure.yaml)
  SECRETS=${SECRETS_DIR:-${KRAKEN_SECRETS_DIR:-$INFRA/.secrets}}
  [ -f "$SECRETS/ca.pem" ] && [ -f "$SECRETS/dashboard.token" ] \
    || fail "SECURE=1 needs the dev secrets in '$SECRETS' (run kraken/infra/scripts/dev-hub-pki.sh, or set SECRETS_DIR)"
  SECRETS=$(cd "$SECRETS" && pwd)
  DASHBOARD_TOKEN=$(cat "$SECRETS/dashboard.token")
  curl() {
    local a to_hub=0
    for a in "$@"; do
      case "$a" in "$HUB"*|"$HUB2"*) to_hub=1 ;; esac
    done
    if [ "$to_hub" = "1" ]; then
      command curl --cacert "$SECRETS/ca.pem" -K <(printf 'header = "Authorization: Bearer %s"\n' "$DASHBOARD_TOKEN") "$@"
    else
      command curl "$@"
    fi
  }
fi

# JWT=1: the JWT overlay (kraken/infra/compose.jwt.yaml) on top of the secure one. It matters here because the flow steps
# recreate service-b and service-c with these files: without it they would come back with static tokens.
if [ "$JWT" = "1" ]; then
  [ "$SECURE" = "1" ] || fail "JWT=1 needs SECURE=1 (the JWT overlay sits on top of the secure sandbox)"
  COMPOSE_FILES+=(-f compose.jwt.yaml)
fi

# MTLS=1: the mTLS overlay (kraken/infra/compose.mtls.yaml) on top of the secure one; the flow steps recreate service-b and
# service-c with these files, and service-c must come back with its certificate. It is not combinable with JWT=1: both
# overlays rewrite service-c's credentials.
if [ "$MTLS" = "1" ]; then
  [ "$SECURE" = "1" ] || fail "MTLS=1 needs SECURE=1 (the mTLS overlay sits on top of the secure sandbox)"
  [ "$JWT" = "0" ] || fail "MTLS=1 and JWT=1 exclude each other (both overlays rewrite service-c's credentials)"
  COMPOSE_FILES+=(-f compose.mtls.yaml)
fi

# Recreates service-b and service-c from the sandbox compose files plus the given extra files, without touching
# their dependencies, and waits until both are registered with permits matching <pattern>.
recreate_b_and_c() { # <permits pattern> [extra compose file ...]
  local pattern=$1 i topo b c
  shift
  local files=("${COMPOSE_FILES[@]}")
  for f in "$@"; do files+=(-f "$f"); done
  (cd "$INFRA" && docker compose "${files[@]}" --profile examples up -d --no-deps --no-build service-b service-c) \
    >/dev/null 2>&1 || fail "docker compose could not recreate service-b and service-c in $INFRA"
  for i in $(seq 1 120); do
    topo=$(curl -fsS "$HUB/api/v1/topology" 2>/dev/null || true)
    b=$(instance_fields "$topo" service-b)
    c=$(instance_fields "$topo" service-c)
    if echo "$b" | grep -Eq "\"permitsAvailable\":$pattern," && echo "$c" | grep -Eq "\"permitsAvailable\":$pattern," \
       && echo "$topo" | grep -q '"type":"examples.CountGreetings","kind":"QUERY","liveInstances":1,[^}]*"ownerServices":\["service-b"\]'; then
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
# Splitting at '{' keeps it to grep and tr: the type arrays before "permitsAvailable" rule out a single regex.
# Empty (never a failure, which pipefail and set -e would turn into a silent exit) while the service is not listed.
instance_fields() { # <topology json> <service>
  printf '%s' "$1" | tr '{' '\n' | { grep -A1 "^\"serviceName\":\"$2\"" || true; } | tail -n 1
}

# After a burst the permits must come back (a lost permit would stall the instance): permits available (the
# sandbox's one), nothing queued.
expect_permit_back() { # <service>
  local i fields
  for i in $(seq 1 20); do
    fields=$(instance_fields "$(curl -fsS "$HUB/api/v1/topology" 2>/dev/null || true)" "$1")
    if echo "$fields" | grep -q '"permitsAvailable":[1-9][0-9]*,"queued":0'; then return 0; fi
    sleep 0.1
  done
  fail "$1 does not have its permit back on the hub: $fields"
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
    queued=$(instance_fields "$(curl -fsS "$HUB/api/v1/topology" 2>/dev/null || true)" service-b \
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
  # /stream/rows -> "<count> <service> <rows are n = 1..count in order> <distinct service/instance pairs>".
  rows_summary() {
    jq -r '"\(.count) \(.service // "-") \((.rows | map(.n)) == [range(1; (.rows | length) + 1)]) \([.rows[] | "\(.service)/\(.instance)"] | unique | length)"' 2>/dev/null || true
  }
  # /api/v1/types -> "<shared> <owners, comma-separated>" of the type named by $1; empty while it is not listed.
  type_owners() {
    jq -r --arg t "$1" '.types[] | select(.type == $t) | "\(.shared) \(.owners | join(","))"' 2>/dev/null || true
  }
  # /subscribe/counter -> "<initial service> <initial value> <updates> <values of service-b's updates> <values of service-c's>"
  # (values comma-separated in arrival order, "-" for none).
  counter_summary() {
    jq -r '"\(.initial.service) \(.initial.value) \(.updates | length) \([.updates[] | select(.service == "service-b") | .value | tostring] | join(",") | if . == "" then "-" else . end) \([.updates[] | select(.service == "service-c") | .value | tostring] | join(",") | if . == "" then "-" else . end)"' 2>/dev/null || true
  }
  # /subscribe/open -> "<instances> <sum of open> <service=open, comma-separated>".
  open_summary() {
    jq -r '"\(.instances | length) \([.instances[].open] | add // 0) \([.instances[] | "\(.service)=\(.open)"] | sort | join(","))"' 2>/dev/null || true
  }
  # /api/v1/topology -> "<totals.inFlight> <sum over every instance of inFlightAsHandler + inFlightAsSender>".
  in_flight() {
    jq -r '"\(.totals.inFlight // 0) \([.services[].instances[] | (.inFlightAsHandler // 0) + (.inFlightAsSender // 0)] | add // 0)"' 2>/dev/null || true
  }
  # /api/v1/processors -> "<state> <groupId> <deadLetters> <assignedPartitions> <instancesReporting>" of the processor named $2
  # of the service named $1; empty while it is not listed.
  processor_summary() {
    jq -r --arg s "$1" --arg p "$2" '.services[] | select(.serviceName == $s) | .processors[] | select(.name == $p)
      | "\(.state) \(.groupId) \(.deadLetters) \(.assignedPartitions) \(.instancesReporting)"' 2>/dev/null || true
  }
  # /api/v1/processors -> "<state> <replaying> <operationId or ->" of the processor named $2 of the service named $1.
  control_state() {
    jq -r --arg s "$1" --arg p "$2" '.services[] | select(.serviceName == $s) | .processors[] | select(.name == $p)
      | "\(.state) \(.replaying) \(if (.operationId // "") == "" then "-" else .operationId end)"' 2>/dev/null || true
  }
  # A top-level field of a control reply as text (empty when absent): reply_field result.
  reply_field() { jq -r --arg f "$1" 'if has($f) then .[$f] | tostring else empty end' 2>/dev/null || true; }
  # GET .../dead-letters through the hub -> one line per letter, oldest first: "<id> <key> <reason> <attempts>".
  hub_letters() { jq -r '.letters[] | "\(.id) \(.key) \(.reason) \(.attempts)"' 2>/dev/null || true; }
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
  rows_summary() { python3 -I -c 'import json,sys
try:
    r = json.load(sys.stdin); rows = r.get("rows") or []
    ordered = [x.get("n") for x in rows] == list(range(1, len(rows) + 1))
    print(r.get("count"), r.get("service") or "-", str(ordered).lower(), len({(x.get("service"), x.get("instance")) for x in rows}))
except Exception: pass' || true; }
  type_owners() { python3 -I -c 'import json,sys
try:
    for t in json.load(sys.stdin).get("types", []):
        if t.get("type") == sys.argv[1]:
            print(str(bool(t.get("shared"))).lower(), ",".join(t.get("owners") or []))
except Exception: pass' "$1" || true; }
  counter_summary() { python3 -I -c 'import json,sys
try:
    r = json.load(sys.stdin); i = r["initial"]; u = r.get("updates") or []
    vals = lambda svc: ",".join(str(x["value"]) for x in u if x.get("service") == svc) or "-"
    print(i["service"], i["value"], len(u), vals("service-b"), vals("service-c"))
except Exception: pass' || true; }
  open_summary() { python3 -I -c 'import json,sys
try:
    r = json.load(sys.stdin); ins = r.get("instances") or []
    print(len(ins), sum(x.get("open") or 0 for x in ins), ",".join(sorted("%s=%s" % (x["service"], x.get("open")) for x in ins)))
except Exception: pass' || true; }
  in_flight() { python3 -I -c 'import json,sys
try:
    t = json.load(sys.stdin)
    per = sum((i.get("inFlightAsHandler") or 0) + (i.get("inFlightAsSender") or 0) for s in t.get("services", []) for i in s.get("instances", []))
    print((t.get("totals") or {}).get("inFlight") or 0, per)
except Exception: pass' || true; }
  processor_summary() { python3 -I -c 'import json,sys
try:
    for s in json.load(sys.stdin).get("services", []):
        if s.get("serviceName") == sys.argv[1]:
            for p in s.get("processors", []):
                if p.get("name") == sys.argv[2]:
                    print(p["state"], p["groupId"], p["deadLetters"], p["assignedPartitions"], p["instancesReporting"])
except Exception: pass' "$1" "$2" || true; }
  control_state() { python3 -I -c 'import json,sys
try:
    for s in json.load(sys.stdin).get("services", []):
        if s.get("serviceName") == sys.argv[1]:
            for p in s.get("processors", []):
                if p.get("name") == sys.argv[2]:
                    print(p["state"], "true" if p.get("replaying") else "false", p.get("operationId") or "-")
except Exception: pass' "$1" "$2" || true; }
  reply_field() { python3 -I -c 'import json,sys
try:
    v = json.load(sys.stdin)[sys.argv[1]]
    print(json.dumps(v) if isinstance(v, (bool, list, dict)) or v is None else v)
except Exception: pass' "$1" || true; }
  hub_letters() { python3 -I -c 'import json,sys
try:
    for l in json.load(sys.stdin).get("letters", []):
        print(l["id"], l["key"], l["reason"], l["attempts"])
except Exception: pass' || true; }
else
  hub_id_of() { fail "jq or python3 needed for the two-hub steps (or set SKIP_HA=1)"; }
  example_instances() { hub_id_of; }
  peers_of() { hub_id_of; }
  inventory_type() { fail "jq or python3 needed for the scatter-gather steps (or set SKIP_SCATTER=1)"; }
  scatter_summary() { inventory_type; }
  service_of() { inventory_type; }
  rows_summary() { fail "jq or python3 needed for the streaming-query steps (or set SKIP_STREAM=1)"; }
  type_owners() { fail "jq or python3 needed for the subscription-query steps (or set SKIP_SUBSCRIBE=1)"; }
  counter_summary() { type_owners; }
  open_summary() { type_owners; }
  in_flight() { rows_summary; }
  processor_summary() { fail "jq or python3 needed for the processor steps (or set SKIP_AUDIT=1)"; }
  control_state() { fail "jq or python3 needed for the processor-control steps (or set SKIP_CONTROL=1)"; }
  reply_field() { control_state; }
  hub_letters() { control_state; }
fi

# Succeeds (prints nothing) when both hubs list the same instances of service-a, service-b and service-c, each of
# them at least once, and every instance reports both hub ids as connected; else prints why.
both_hubs_problem() {
  local t1 t2 id1 id2 l1 l2 svc
  t1=$(curl -fsS --max-time 3 "$HUB/api/v1/topology" 2>/dev/null || true)
  t2=$(curl -fsS --max-time 3 "$HUB2/api/v1/topology" 2>/dev/null || true)
  id1=$(printf '%s' "$t1" | hub_id_of); id2=$(printf '%s' "$t2" | hub_id_of)
  [ -n "$id1" ] || { echo "$HUB/api/v1/topology unreachable"; return; }
  [ -n "$id2" ] || { echo "$HUB2/api/v1/topology unreachable"; return; }
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
  instance_fields "$(curl -fsS "$1/api/v1/topology" 2>/dev/null || true)" "$2" | sed -n 's/.*"permitsAvailable":\([0-9]*\).*/\1/p'
}

ha_steps() {
  local h2=${HUB2_CONTAINER:-kraken-hub-2} secs peers p out stop t0 pid stopped_at gone_at total failed max late_failed after
  local share permits topped_at
  command -v docker >/dev/null || fail "docker CLI needed for the two-hub steps (or set SKIP_HA=1)"
  docker inspect "$h2" >/dev/null 2>&1 || fail "container $h2 not found (set HUB2_CONTAINER, or SKIP_HA=1)"

  echo "15. both hubs ($HUB, $HUB2) list every example instance, each reporting both hub ids ..."
  secs=$(await_both_hubs 15)
  echo "   on both hubs after ${secs}s: $(curl -fsS "$HUB/api/v1/topology" | example_instances | cut -d' ' -f1,3 | sort | uniq -c | tr -s ' ' | tr '\n' ';')"

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
    (cd "$INFRA" && docker compose "${COMPOSE_FILES[@]}" --profile examples \
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

# P4b (ADR 0017): examples.Rows is a shared streaming query type owned by service-b (Kotlin) and service-c (Rust); a
# stream from service-a goes to one instance of either, paced by credits. Step 19 streams 1000 rows with a window of
# 8 credits (so the handler is held back and refilled many times) until both owners streamed once; 19b cancels a stream
# from the collector's side (take=50) and expects both hubs' in-flight counts back at 0; 19c makes the handler fail on
# row 3 and expects the handler's error as 502. Needs jq or python3; SKIP_STREAM=1 skips these steps, ONLY_STREAM=1
# runs only them.
stream_steps() {
  local hubs=("$HUB") h i body summary count svc ordered responders seen="" t code
  [ "${SKIP_HA:-0}" = "1" ] || hubs+=("$HUB2")
  echo "19. GET /stream/rows?count=1000&credit=8 until service-b and service-c both streamed: 1000 rows in order (n = 1..1000) from one instance ..."
  for i in $(seq 1 12); do
    body=$(curl -fsS --max-time 30 "$A/stream/rows?count=1000&credit=8") \
      || fail "GET /stream/rows failed (call $i): $(curl -s --max-time 30 "$A/stream/rows?count=1000&credit=8" | head -c 300)"
    summary=$(printf '%s' "$body" | rows_summary)
    read -r count svc ordered responders <<< "$summary"
    [ "$count" = "1000" ] || fail "expected 1000 rows, got '$count' (call $i): $(printf '%s' "$body" | head -c 300)"
    [ "$ordered" = "true" ] || fail "rows are not n = 1..1000 in order (call $i): $(printf '%s' "$body" | head -c 300)"
    [ "$responders" = "1" ] || fail "expected one streaming instance, got $responders (call $i): $(printf '%s' "$body" | head -c 300)"
    seen="$seen $svc"
    case " $seen " in *" service-b "*) case " $seen " in *" service-c "*) break ;; esac ;; esac
  done
  echo "   streamed by:$seen (1000 rows each, in order, one instance per stream)"
  for svc in service-b service-c; do
    case " $seen " in *" $svc "*) ;; *) fail "twelve streams never reached $svc (streamed by:$seen)" ;; esac
  done

  echo "19b. GET /stream/rows?count=1000&credit=8&take=50: the collector cancels after 50 rows; the hubs' in-flight counts go back to 0 ..."
  body=$(curl -fsS --max-time 30 "$A/stream/rows?count=1000&credit=8&take=50") \
    || fail "GET /stream/rows?take=50 failed: $(curl -s --max-time 30 "$A/stream/rows?count=1000&credit=8&take=50" | head -c 300)"
  summary=$(printf '%s' "$body" | rows_summary)
  read -r count svc ordered responders <<< "$summary"
  [ "$count" = "50" ] && [ "$ordered" = "true" ] || fail "expected 50 rows in order, got '$summary': $(printf '%s' "$body" | head -c 300)"
  echo "   $count rows from $svc, then cancelled"
  for h in "${hubs[@]}"; do
    for i in $(seq 1 20); do
      t=$(curl -fsS "$h/api/v1/topology" 2>/dev/null | in_flight)
      [ "$t" = "0 0" ] && break
      [ "$i" -eq 20 ] && fail "$h still has requests in flight 10 s after the cancelled stream (totals.inFlight, per-instance sum: '$t')"
      sleep 0.5
    done
    echo "   $h: sanity check, totals.inFlight ${t%% *}, per-instance in-flight sum ${t#* } (blind to subscriptions after their initial part)"
  done

  echo "19c. GET /stream/rows?count=10&failAt=3: the handler fails on row 3 (after 2 rows) and the error maps to 502 RemoteHandlerException ..."
  body=$(curl -sS --max-time 30 -w '\n%{http_code}' "$A/stream/rows?count=10&failAt=3") || fail "GET /stream/rows?failAt=3 failed: $body"
  code=${body##*$'\n'}; body=${body%$'\n'*}
  [ "$code" = "502" ] || fail "expected HTTP 502 for the failed stream, got $code: $body"
  printf '%s' "$body" | grep -q '"error":"RemoteHandlerException"' || fail "expected RemoteHandlerException: $body"
  printf '%s' "$body" | grep -q 'Rows failed at row 3' || fail "expected the handler's message 'Rows failed at row 3': $body"
  echo "   $code $body"
}

# P4c (ADR 0017): examples.WatchCounter is a shared subscription query type served by service-b (Kotlin) and service-c
# (Rust), each with an in-memory counter per name. Step 20 opens `GET /subscribe/counter?updates=4` in the background
# (service-a subscribes, collects the initial result and four updates, cancels), waits until both owners hold the
# subscription, then increments the counter twice through service-b (`POST $B/counters/<name>/increment`) and twice
# through service-c (the command examples.IncrementCounter, sent by `POST $A/counters/<name>/increment`), and expects
# the initial result and four updates, two from each service, increasing in value per service. The initial value is
# whatever the answering service's counter was when it read it: 0, or higher when an increment slipped in between the
# registration of the subscription and that read (the increment is then in the initial value *and* delivered as an
# update, never lost), so step 20 accepts 0..2 for it and still demands the updates 1,2 from each service. Step 20b checks that no
# subscription is left behind once the request returned. The proof is the owners' open counts (black-box: the hubs have
# no metrics endpoint, so it asks the owners through the shared query examples.OpenSubscriptions,
# `GET $A/subscribe/open`, which counts every subscription an instance serves) returning to what they were before step
# 20: only a hub Cancel to each target removes a subscription there. The hubs' in-flight counts at 0 are a secondary
# sanity check: a hub excludes a subscription from them after its initial part, by design, so they cannot see a
# subscription that is left over. Step 20b also checks that a follow-up subscription still gets the counters' current value; step 20c subscribes to a name nobody
# incremented and expects the initial result with the value 0. Needs jq or python3; SKIP_SUBSCRIBE=1 skips these steps,
# ONLY_SUBSCRIBE=1 runs only them.
subscribe_steps() {
  local hubs=("$HUB") h i t body out pid summary base base_open open_now instances sum detail
  local name="smoke$(date +%s)" unknown="unknown$(date +%s)"
  local initial_service initial_value n b_values c_values
  [ "${SKIP_HA:-0}" = "1" ] || hubs+=("$HUB2")
  echo "20a. /api/v1/types on ${hubs[*]}: examples.WatchCounter is shared, served by service-b and service-c ..."
  for h in "${hubs[@]}"; do
    for i in $(seq 1 60); do
      t=$(curl -fsS "$h/api/v1/types" 2>/dev/null | type_owners examples.WatchCounter)
      [ "$t" = "true service-b,service-c" ] && break
      [ "$i" -eq 60 ] && fail "examples.WatchCounter on $h/api/v1/types is '$t', expected 'true service-b,service-c' (shared, two owners)"
      sleep 1
    done
    echo "   $h: shared=${t%% *} owners=${t#* }"
  done

  base=$(curl -fsS --max-time 10 "$A/subscribe/open" | open_summary)
  read -r instances sum base_open <<< "$base"
  [ "$instances" = "2" ] || fail "expected the open-subscription counts of two owner instances before step 20, got '$base'"
  echo "   open subscriptions before: $base_open (sum $sum)"

  echo "20. GET /subscribe/counter?name=$name&updates=4 in the background; 2 increments through service-b and 2 through service-c ..."
  out=$(mktemp)
  curl -sS --max-time 30 "$A/subscribe/counter?name=$name&updates=4&timeoutMs=10000" > "$out" 2>&1 &
  pid=$!
  # The request returns only after the updates, so its subscription is seen from outside: both owners hold one more.
  for i in $(seq 1 100); do
    summary=$(curl -fsS --max-time 5 "$A/subscribe/open" 2>/dev/null | open_summary)
    read -r instances n detail <<< "${summary:-0 0 -}"
    [ "$instances" = "2" ] && [ "$n" -ge $((sum + 2)) ] && break
    kill -0 "$pid" 2>/dev/null || break
    [ "$i" -eq 100 ] && { kill "$pid" 2>/dev/null || true; fail "the subscription never showed on both owners (open: ${summary:-none}, before: $base_open)"; }
    sleep 0.1
  done
  for i in 1 2; do
    curl -fsS -X POST "$B/counters/$name/increment" >/dev/null || fail "POST $B/counters/$name/increment failed (service-b)"
  done
  for i in 1 2; do
    curl -fsS -X POST "$A/counters/$name/increment" >/dev/null || fail "POST $A/counters/$name/increment failed (service-c through the hub)"
  done
  wait "$pid" || fail "GET /subscribe/counter failed: $(cat "$out")"
  body=$(cat "$out"); rm -f "$out"
  echo "   $body"
  summary=$(printf '%s' "$body" | counter_summary)
  read -r initial_service initial_value n b_values c_values <<< "$summary"
  [ -n "$summary" ] || fail "no initial result and updates in: $body"
  case "$initial_service" in service-b|service-c) ;; *) fail "the initial result came from '$initial_service': $body" ;; esac
  # The initial value is the answering service's counter when it read it: the increments start once both owners hold the
  # subscription, so one may land before that read (0 up to the 2 sent to that service). The subscription is registered
  # before the initial is read, so such an increment is also delivered as an update: it is never lost, only seen twice.
  case "$initial_value" in 0|1|2) ;; *) fail "expected an initial value from 0 to 2 (the increments sent to $initial_service so far), got '$initial_value': $body" ;; esac
  [ "$n" = "4" ] || fail "expected 4 updates, got $n: $body"
  # Each service's counter counts 1, 2: strictly increasing per service (the order between the two services is not defined),
  # and every increment is an update, whether or not the initial value already included it. That is what a lost update
  # (the race of the registration against the initial read) would break.
  [ "$b_values" = "1,2" ] || fail "expected service-b's updates 1,2 in order, got '$b_values': $body"
  [ "$c_values" = "1,2" ] || fail "expected service-c's updates 1,2 in order, got '$c_values': $body"
  echo "   initial from $initial_service (value $initial_value, 0 to 2 accepted); 4 updates: service-b $b_values, service-c $c_values"

  echo "20b. nothing is left subscribed: the owners' open counts are back to $base_open, proving the hub's Cancel reached every target ..."
  for i in $(seq 1 50); do
    summary=$(curl -fsS --max-time 5 "$A/subscribe/open" 2>/dev/null | open_summary)
    read -r instances n detail <<< "${summary:-0 0 -}"
    [ "$detail" = "$base_open" ] && break
    [ "$i" -eq 50 ] && fail "after the request the owners still hold subscriptions: '${summary:-none}', before step 20: '$base_open'"
    sleep 0.2
  done
  echo "   open subscriptions after: $detail (the proof: the owners dropped them on the hub's Cancel)"
  # Secondary sanity check only: the hub does not count a subscription as in flight after its initial part, so this cannot
  # see a subscription left behind (the owners' counts above do); it catches a stuck initial request or a stuck permit.
  for h in "${hubs[@]}"; do
    for i in $(seq 1 20); do
      t=$(curl -fsS "$h/api/v1/topology" 2>/dev/null | in_flight)
      [ "$t" = "0 0" ] && break
      [ "$i" -eq 20 ] && fail "$h still has requests in flight after the subscription was cancelled (totals.inFlight, per-instance sum: '$t')"
      sleep 0.5
    done
    echo "   $h: totals.inFlight ${t%% *}, per-instance in-flight sum ${t#* }"
  done
  body=$(curl -fsS --max-time 15 "$A/subscribe/counter?name=$name&updates=0&timeoutMs=10000") || fail "the follow-up GET /subscribe/counter failed"
  summary=$(printf '%s' "$body" | counter_summary)
  read -r initial_service initial_value n b_values c_values <<< "$summary"
  [ "$initial_value" = "2" ] && [ "$n" = "0" ] || fail "the follow-up subscription should see the counter at 2 and no update, got '$summary': $body"
  echo "   a follow-up subscription and cancel still work: initial $initial_service value $initial_value"
  for i in $(seq 1 50); do
    summary=$(curl -fsS --max-time 5 "$A/subscribe/open" 2>/dev/null | open_summary)
    read -r instances n detail <<< "${summary:-0 0 -}"
    [ "$detail" = "$base_open" ] && break
    [ "$i" -eq 50 ] && fail "the follow-up subscription was not cancelled: '${summary:-none}', expected '$base_open'"
    sleep 0.2
  done

  echo "20c. GET /subscribe/counter?name=$unknown: a name nobody incremented still gets its initial result, value 0 ..."
  body=$(curl -fsS --max-time 15 "$A/subscribe/counter?name=$unknown&updates=0&timeoutMs=10000") || fail "GET /subscribe/counter for an unknown name failed"
  summary=$(printf '%s' "$body" | counter_summary)
  read -r initial_service initial_value n b_values c_values <<< "$summary"
  [ "$initial_value" = "0" ] && [ "$n" = "0" ] || fail "expected the initial value 0 and no update for an unknown name, got '$summary': $body"
  echo "   $body"
}

# S1 (sagas, ADR 0021): service-b's GreetingDeliverySaga gets every delivered greeting printed by the fake printer
# (kraken/infra, localhost:8093), which answers by the greeting name's prefix. Each step greets with a name of its
# kind through service-a, then reads `GET $B/postcards/<id>` (service-b's projection of PostcardPrinted/PostcardFailed,
# 404 while the postcard is in flight) until it is printed or failed. Step 24: webhook-* (the printer posts a signed
# webhook; replaying it is a 200 no-op and a wrong secret is a 401). 24b: topic-* (the printer produces to printer.events).
# 24c: poll-* (the saga polls the printer every 2 s). 24d: silent-* (the printer never answers: PostcardFailed TIMEOUT after
# the saga's 30 s). 24e: crash-* (the printer accepts and dies before replying: PostcardFailed UNKNOWN_OUTCOME, and compose
# brings the printer back). The shared webhook secret is the sandbox's dev-only value. SKIP_POSTCARD=1 skips these steps,
# ONLY_POSTCARD=1 runs only them; PRINTER=http://localhost:8093 is the printer's address, PRINTER_WEBHOOK_SECRET its secret.
PRINTER=${PRINTER:-http://localhost:8093}
PRINTER_WEBHOOK_SECRET=${PRINTER_WEBHOOK_SECRET:-dev-only-printer-webhook-secret}

# Sends a greeting named <name> through service-a and prints the id (the job id of its postcard).
greet() { # <name>
  local body
  body=$(curl -fsS -X POST "$A/greetings" -H 'content-type: application/json' -d "{\"name\":\"$1\"}") || fail "POST /greetings for '$1' failed"
  echo "$body" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p'
}

# Waits until the postcard <id> has an outcome and prints it; fails when it is not <status> (with <reason> when given) within
# <seconds>. The elapsed seconds are left in POSTCARD_SECONDS.
await_postcard() { # <id> <PRINTED|FAILED> <reason or -> <seconds>
  local id=$1 status=$2 reason=$3 limit=$4 body="" start=$SECONDS
  while [ $((SECONDS - start)) -lt "$limit" ]; do
    body=$(curl -sS "$B/postcards/$id" 2>/dev/null || true)
    if echo "$body" | grep -q '"jobId"'; then break; fi
    sleep 1
  done
  POSTCARD_SECONDS=$((SECONDS - start))
  echo "$body" | grep -q "\"status\":\"$status\"" || fail "postcard $id: expected $status within ${limit}s, got '${body:-nothing}'"
  [ "$reason" = "-" ] || echo "$body" | grep -q "\"reason\":\"$reason\"" || fail "postcard $id: expected reason $reason, got: $body"
  echo "   postcard $id: $body (after ${POSTCARD_SECONDS}s)"
}

postcard_steps() {
  local run id body code
  run=$(date +%s)

  echo "24. webhook-$run: the printer posts a signed webhook to service-b; a replay is a no-op, a wrong secret is refused ..."
  id=$(greet "webhook-$run"); [ -n "$id" ] || fail "no id in the greeting reply"
  await_postcard "$id" PRINTED - 90
  body="{\"deliveryId\":\"printed-$id\",\"jobId\":\"$id\",\"name\":\"webhook-$run\"}"
  code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$B/printer/webhook" -H 'content-type: application/json' \
    -H "X-Kraken-Webhook-Secret: $PRINTER_WEBHOOK_SECRET" -d "$body") || fail "replaying the webhook failed"
  [ "$code" = "200" ] || fail "a replayed webhook delivery should be answered 200, got $code"
  code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$B/printer/webhook" -H 'content-type: application/json' \
    -H 'X-Kraken-Webhook-Secret: not-the-secret' -d "$body") || fail "posting a webhook with a wrong secret failed"
  [ "$code" = "401" ] || fail "a webhook with a wrong secret should be answered 401, got $code"
  echo "   replay -> 200, wrong secret -> 401"

  echo "24b. topic-$run: the printer produces to printer.events, service-b translates the record ..."
  id=$(greet "topic-$run"); [ -n "$id" ] || fail "no id in the greeting reply"
  await_postcard "$id" PRINTED - 60

  echo "24c. poll-$run: nobody tells service-b; its saga polls the printer every 2 s until the job is done ..."
  id=$(greet "poll-$run"); [ -n "$id" ] || fail "no id in the greeting reply"
  await_postcard "$id" PRINTED - 60
  curl -fsS "$PRINTER/jobs/$id" | grep -q '"status":"DONE"' || fail "the printer's job $id should be DONE after the saga polled it"

  echo "24d. silent-$run: the printer never answers, the saga's 30 s timeout publishes PostcardFailed TIMEOUT ..."
  id=$(greet "silent-$run"); [ -n "$id" ] || fail "no id in the greeting reply"
  await_postcard "$id" FAILED TIMEOUT 60
  [ "$POSTCARD_SECONDS" -ge 25 ] || fail "the timeout fired after ${POSTCARD_SECONDS}s, before the 30 s the saga gives the printer"

  echo "24e. crash-$run: the printer accepts and dies before replying, the outcome is unknown: PostcardFailed UNKNOWN_OUTCOME ..."
  id=$(greet "crash-$run"); [ -n "$id" ] || fail "no id in the greeting reply"
  await_postcard "$id" FAILED UNKNOWN_OUTCOME 60
  [ "$POSTCARD_SECONDS" -lt 25 ] || fail "the unknown outcome took ${POSTCARD_SECONDS}s: it should be reported at once, not by the 30 s timeout"
  echo "   waiting for compose to bring the printer back ..."
  for i in $(seq 1 60); do
    curl -fsS "$PRINTER/actuator/health" 2>/dev/null | grep -q '"status":"UP"' && break
    [ "$i" -eq 60 ] && fail "the printer did not come back within 60s after crashing"
    sleep 1
  done
}

# S2 (Rust sagas, ADR 0021): service-c's banner saga (examples.Banner) gets every shouted text printed as a banner by the
# same fake printer, which answers service-c where each job asks it to (its own webhook and printer.banner.events, never
# service-b). Each step shouts a text of its kind through service-a (`POST $A/shouts`, the command examples.Shout to
# service-c, whose TextShouted starts the saga) and reads `GET $C/banners/banner-<text>` (service-c's projection of
# BannerPrinted/BannerFailed, 404 while the banner is in flight) until it is printed or failed. Step 25: webhook-* (a
# replay is a 200 no-op, a wrong secret a 401). 25b: topic-* (printer.banner.events, the external source printer-banner).
# 25c: poll-* (the saga polls every 2 s). 25d: silent-* (BannerFailed TIMEOUT after 30 s). 25e: crash-* (BannerFailed
# UNKNOWN_OUTCOME at once; compose brings the printer back). SKIP_BANNER=1 skips these steps, ONLY_BANNER=1 runs only them.
C=${C:-http://localhost:8094}

# Shouts <text> through service-a (service-c's TextShouted starts the banner saga); fails unless it is shouted back.
shout() { # <text>
  local body
  body=$(curl -fsS -X POST "$A/shouts" -H 'content-type: application/json' -d "{\"text\":\"$1\"}") || fail "POST /shouts for '$1' failed"
  echo "$body" | grep -q '"text"' || fail "no text in the shout reply: $body"
}

# Waits until the banner <job id> has an outcome; as await_postcard, the elapsed seconds are left in BANNER_SECONDS.
await_banner() { # <job id> <PRINTED|FAILED> <reason or -> <seconds>
  local id=$1 status=$2 reason=$3 limit=$4 body="" start=$SECONDS
  while [ $((SECONDS - start)) -lt "$limit" ]; do
    body=$(curl -sS "$C/banners/$id" 2>/dev/null || true)
    if echo "$body" | grep -q '"jobId"'; then break; fi
    sleep 1
  done
  BANNER_SECONDS=$((SECONDS - start))
  echo "$body" | grep -q "\"status\":\"$status\"" || fail "banner $id: expected $status within ${limit}s, got '${body:-nothing}'"
  [ "$reason" = "-" ] || echo "$body" | grep -q "\"reason\":\"$reason\"" || fail "banner $id: expected reason $reason, got: $body"
  echo "   banner $id: $body (after ${BANNER_SECONDS}s)"
}

banner_steps() {
  local run id body code
  run=$(date +%s)

  echo "25. webhook-$run: the printer posts a signed webhook to service-c (Rust); a replay is a no-op, a wrong secret is refused ..."
  shout "webhook-$run"; id="banner-webhook-$run"
  await_banner "$id" PRINTED - 90
  body="{\"deliveryId\":\"printed-$id\",\"jobId\":\"$id\",\"name\":\"webhook-$run\"}"
  code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$C/printer/webhook" -H 'content-type: application/json' \
    -H "X-Kraken-Webhook-Secret: $PRINTER_WEBHOOK_SECRET" -d "$body") || fail "replaying the banner webhook failed"
  [ "$code" = "200" ] || fail "a replayed banner webhook delivery should be answered 200, got $code"
  code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$C/printer/webhook" -H 'content-type: application/json' \
    -H 'X-Kraken-Webhook-Secret: not-the-secret' -d "$body") || fail "posting a banner webhook with a wrong secret failed"
  [ "$code" = "401" ] || fail "a banner webhook with a wrong secret should be answered 401, got $code"
  echo "   replay -> 200, wrong secret -> 401"

  echo "25b. topic-$run: the printer produces to printer.banner.events, service-c's printer-banner source translates the record ..."
  shout "topic-$run"; id="banner-topic-$run"
  await_banner "$id" PRINTED - 60

  echo "25c. poll-$run: nobody tells service-c; its saga polls the printer every 2 s until the job is done ..."
  shout "poll-$run"; id="banner-poll-$run"
  await_banner "$id" PRINTED - 60
  curl -fsS "$PRINTER/jobs/$id" | grep -q '"status":"DONE"' || fail "the printer's job $id should be DONE after the saga polled it"

  echo "25d. silent-$run: the printer never answers, the saga's 30 s timeout publishes BannerFailed TIMEOUT ..."
  shout "silent-$run"; id="banner-silent-$run"
  await_banner "$id" FAILED TIMEOUT 60
  [ "$BANNER_SECONDS" -ge 25 ] || fail "the timeout fired after ${BANNER_SECONDS}s, before the 30 s the saga gives the printer"

  echo "25e. crash-$run: the printer accepts and dies before replying, the outcome is unknown: BannerFailed UNKNOWN_OUTCOME ..."
  shout "crash-$run"; id="banner-crash-$run"
  await_banner "$id" FAILED UNKNOWN_OUTCOME 60
  [ "$BANNER_SECONDS" -lt 25 ] || fail "the unknown outcome took ${BANNER_SECONDS}s: it should be reported at once, not by the 30 s timeout"
  echo "   waiting for compose to bring the printer back ..."
  for i in $(seq 1 60); do
    curl -fsS "$PRINTER/actuator/health" 2>/dev/null | grep -q '"status":"UP"' && break
    [ "$i" -eq 60 ] && fail "the printer did not come back within 60s after crashing"
    sleep 1
  done
}

# P6a (ADR 0022): service-b (Kotlin) and service-c (Rust) each run a named event processor, `audit` (consumer group
# `<service>.audit`, markers `processor:audit:<eventId>`), next to their `default` one. It handles an audit note published
# under a key (`POST <service>/audit/notes {"key","text"}`, service-b's AuditNote and service-c's ShoutAudit) and fails
# on demand: while `POST <service>/audit/failing?on=true` is on, a note whose text starts with `poison` fails. The example
# admin endpoints over the library's DeadLetters (`GET /audit/dead-letters`, `POST /audit/dead-letters/{id}/retry`,
# `POST /audit/dead-letters/retry-all`, `DELETE /audit/dead-letters/{id}`) are the same in both. Step 26: both audit
# processors run (RUNNING, with partitions) on the hub's `GET /api/v1/processors`, next to `default`, and no letters are
# left over. 26b / 26c: with the failure on, service-b / service-c get two poison notes under one key and a fine note under
# another: the first is parked HANDLER_FAILED, the second SEQUENCED behind it, the other key is handled. 26d: the hub shows
# both processors FAILING with their two dead letters (sampled by the service, so within a minute). 26e: retrying the head
# while the failure is on answers Failed, retrying the sequenced letter answers Blocked behind the head. 26f: with the
# failure off, retrying drains the key in order (service-b: the head, then retryAll; service-c: the head, then the second)
# and no letter is left. 26g: a poison note is parked and discarded (a second discard is a 404), and its key flows again.
# 26h: the hub's dead-letter count is back to 0. SKIP_AUDIT=1 skips these steps, ONLY_AUDIT=1 runs only them.

# The `audit` letters at <base> (service-b's or service-c's HTTP address), one per line, oldest first:
# "<id> <key> <reason> <attempts> <blockedBy or ->". The views are flat JSON objects, so grep and sed read them.
audit_letters() { # <base>
  curl -fsS "$1/audit/dead-letters?processor=audit" | { grep -o '{[^{}]*}' || true; } \
    | sed -En 's/.*"id":"([^"]*)".*"key":("[^"]*"|null).*"reason":"([^"]*)","attempts":([0-9]+).*"blockedBy":("[^"]*"|null).*/\1 \2 \3 \4 \5/p' \
    | tr -d '"' | sed 's/ null$/ -/'
}

# Waits until <base> has exactly <count> audit letters and prints them (as audit_letters).
await_letters() { # <base> <count> <seconds>
  local base=$1 want=$2 limit=$3 start=$SECONDS letters="" n=0
  while [ $((SECONDS - start)) -lt "$limit" ]; do
    letters=$(audit_letters "$base" || true)
    n=$(printf '%s' "$letters" | grep -c . || true)
    if [ "$n" -eq "$want" ]; then printf '%s' "$letters"; return 0; fi
    sleep 1
  done
  fail "$base: expected $want dead letter(s) within ${limit}s, got $n: $letters"
}

# The texts the audit processor at <base> handled for <key>, in handling order, comma-separated (empty for none).
audit_handled() { # <base> <key>
  curl -fsS "$1/audit/handled" | { grep -o '{[^{}]*}' || true; } | grep "\"key\":\"$2\"" \
    | sed 's/.*"text":"\([^"]*\)".*/\1/' | paste -sd, - || true
}

# Waits until the texts handled for <key> at <base> are exactly <texts> (comma-separated).
await_handled() { # <base> <key> <texts> <seconds>
  local start=$SECONDS got=""
  while [ $((SECONDS - start)) -lt "$4" ]; do
    got=$(audit_handled "$1" "$2" || true)
    [ "$got" = "$3" ] && return 0
    sleep 1
  done
  fail "$1: expected the audit processor to have handled '$3' for key $2 within ${4}s, got '$got'"
}

audit_note() { # <base> <key> <text>
  curl -fsS -X POST "$1/audit/notes" -H 'content-type: application/json' -d "{\"key\":\"$2\",\"text\":\"$3\"}" >/dev/null \
    || fail "POST $1/audit/notes ($2, $3) failed"
}

audit_failing() { # <base> <true|false>
  curl -fsS -X POST "$1/audit/failing?on=$2" | grep -q "\"failing\":$2" || fail "POST $1/audit/failing?on=$2 did not switch the failure $2"
}

# A JSON string field of a flat reply: audit_field '{"result":"Failed"}' result -> Failed.
audit_field() { # <json> <field>
  printf '%s' "$1" | sed -n "s/.*\"$2\":\"\([^\"]*\)\".*/\1/p"
}

audit_retry() { # <base> <letter id> -> the reply
  curl -fsS -X POST "$1/audit/dead-letters/$2/retry" || fail "POST $1/audit/dead-letters/$2/retry failed"
}

audit_count() { # <base>
  curl -fsS "$1/audit/dead-letters/count?processor=audit" | sed -n 's/.*"count":\([0-9]*\).*/\1/p'
}

# Waits until the hub lists <service>'s <processor> with a state and a dead-letter count matching those patterns (and the group
# <group>) and prints its summary: "<state> <groupId> <deadLetters> <assignedPartitions> <instancesReporting>".
await_processor() { # <service> <processor> <group> <state pattern> <dead letters pattern> <seconds>
  local start=$SECONDS sum=""
  while [ $((SECONDS - start)) -lt "$6" ]; do
    sum=$(curl -fsS "$HUB/api/v1/processors" 2>/dev/null | processor_summary "$1" "$2" || true)
    if printf '%s' "$sum" | grep -Eq "^($4) $3 ($5) "; then printf '%s' "$sum"; return 0; fi
    sleep 1
  done
  fail "$HUB/api/v1/processors: expected $1's processor $2 (group $3) in state $4 with $5 dead letters within ${6}s, got '${sum:-not listed}'"
}

# Leaves nothing behind when a step fails: the failure off on both services.
audit_off() {
  trap - EXIT
  command curl -fsS -X POST "$B/audit/failing?on=false" >/dev/null 2>&1 || true
  command curl -fsS -X POST "$C/audit/failing?on=false" >/dev/null 2>&1 || true
}

# Parks a poison note under <key>-1 twice and puts a fine note under another key at <base>; checks what the processor made of them.
audit_park() { # <label> <base> <run>
  local label=$1 base=$2 run=$3 letters head second id key reason attempts blocked id2 key2 reason2 attempts2 blocked2
  audit_failing "$base" true
  audit_note "$base" "k1-$run" poison-1
  audit_note "$base" "k1-$run" poison-2
  audit_note "$base" "k2-$run" "fine-$run"
  letters=$(await_letters "$base" 2 90)
  head=$(printf '%s\n' "$letters" | sed -n 1p); second=$(printf '%s\n' "$letters" | sed -n 2p)
  read -r id key reason attempts blocked <<< "$head"
  [ "$key" = "k1-$run" ] && [ "$reason" = "HANDLER_FAILED" ] && [ "$attempts" -ge 1 ] && [ "$blocked" = "-" ] \
    || fail "$label: the first poison note should be parked HANDLER_FAILED under k1-$run, got '$head'"
  read -r id2 key2 reason2 attempts2 blocked2 <<< "$second"
  [ "$key2" = "k1-$run" ] && [ "$reason2" = "SEQUENCED" ] && [ "$attempts2" = "0" ] && [ "$blocked2" = "$id" ] \
    || fail "$label: the second poison note should be parked SEQUENCED behind $id, got '$second'"
  await_handled "$base" "k2-$run" "fine-$run" 30
  [ -z "$(audit_handled "$base" "k1-$run")" ] || fail "$label: a note of k1-$run was handled although its key is parked"
  echo "   $label: $id HANDLER_FAILED after $attempts attempts, $id2 SEQUENCED behind it, k2-$run handled"
}

audit_steps() {
  local run letters id id2 n sum reply base label
  run=$(date +%s)
  trap audit_off EXIT

  echo "26. both audit processors run (group <service>.audit) next to default on the hub's /api/v1/processors; no letters left over ..."
  for label in "service-b $B" "service-c $C"; do
    read -r label base <<< "$label"
    sum=$(await_processor "$label" audit "$label\.audit" RUNNING '[0-9]+' 150)
    echo "   $label audit: $sum"
    [ "$(printf '%s' "$sum" | cut -d' ' -f4)" -ge 1 ] || fail "$label's audit processor has no partition assigned: $sum"
    sum=$(await_processor "$label" default "$label\.default" 'RUNNING|STARTING|FAILING' '[0-9]+' 30)
    echo "   $label default: $sum"
    audit_failing "$base" false
    curl -fsS -X POST "$base/audit/dead-letters/retry-all?processor=audit" >/dev/null || fail "retry-all of $label's leftover letters failed"
    for id in $(audit_letters "$base" | cut -d' ' -f1); do
      curl -fsS -X DELETE "$base/audit/dead-letters/$id" >/dev/null || fail "discarding the leftover letter $id of $label failed"
    done
    [ "$(audit_count "$base")" = "0" ] || fail "$label still has audit letters after the clean-up"
  done

  echo "26b. service-b (Kotlin): failure on; two poison notes under one key, a fine note under another: HANDLER_FAILED, SEQUENCED, handled ..."
  audit_park service-b "$B" "$run"
  echo "26c. service-c (Rust): the same ..."
  audit_park service-c "$C" "$run"

  echo "26d. the hub shows both audit processors FAILING with two dead letters each, and default still there ..."
  for label in service-b service-c; do
    sum=$(await_processor "$label" audit "$label\.audit" FAILING 2 90)
    echo "   $label audit: $sum"
    sum=$(curl -fsS "$HUB/api/v1/processors" | processor_summary "$label" default)
    printf '%s' "$sum" | grep -Eq "^[A-Z]+ $label\.default [0-9]+ " || fail "$label's default processor is not listed with group $label.default: '$sum'"
  done

  echo "26e. the failure still on: retrying the head answers Failed (attempts grow), retrying the sequenced letter answers Blocked ..."
  for label in "service-b $B" "service-c $C"; do
    read -r label base <<< "$label"
    letters=$(audit_letters "$base" | grep " k1-$run ")
    read -r id _ _ n _ <<< "$(printf '%s\n' "$letters" | sed -n 1p)"
    id2=$(printf '%s\n' "$letters" | sed -n 2p | cut -d' ' -f1)
    reply=$(audit_retry "$base" "$id")
    [ "$(audit_field "$reply" result)" = "Failed" ] && printf '%s' "$reply" | grep -q 'failing on demand' \
      || fail "$label: retrying the head while failing should answer Failed (failing on demand), got: $reply"
    [ "$(audit_letters "$base" | grep "^$id " | cut -d' ' -f4)" -gt "$n" ] || fail "$label: the head's attempts did not grow after a failed retry"
    reply=$(audit_retry "$base" "$id2")
    [ "$(audit_field "$reply" result)" = "Blocked" ] && [ "$(audit_field "$reply" detail)" = "$id" ] \
      || fail "$label: retrying the sequenced letter should answer Blocked behind $id, got: $reply"
    echo "   $label: head -> Failed, sequenced -> Blocked behind $id"
  done

  echo "26f. the failure off: retrying drains each key in order and leaves no letter (service-b: head, then retryAll; service-c: head, then the second) ..."
  audit_failing "$B" false; audit_failing "$C" false
  for label in "service-b $B" "service-c $C"; do
    read -r label base <<< "$label"
    letters=$(audit_letters "$base" | grep " k1-$run ")
    id=$(printf '%s\n' "$letters" | sed -n 1p | cut -d' ' -f1); id2=$(printf '%s\n' "$letters" | sed -n 2p | cut -d' ' -f1)
    reply=$(audit_retry "$base" "$id")
    [ "$(audit_field "$reply" result)" = "Retried" ] || fail "$label: retrying the head with the failure off should answer Retried, got: $reply"
    if [ "$label" = "service-b" ]; then
      reply=$(curl -fsS -X POST "$base/audit/dead-letters/retry-all?processor=audit")
      printf '%s' "$reply" | grep -q '"retried":1,"failed":0,"blocked":0' || fail "$label: retryAll should retry the one letter left, got: $reply"
    else
      reply=$(audit_retry "$base" "$id2")
      [ "$(audit_field "$reply" result)" = "Retried" ] || fail "$label: retrying the second letter should answer Retried, got: $reply"
    fi
    await_handled "$base" "k1-$run" "poison-1,poison-2" 10
    [ "$(audit_count "$base")" = "0" ] || fail "$label: letters are left after the retries: $(audit_letters "$base")"
    echo "   $label: k1-$run handled as poison-1,poison-2, no letter left"
  done

  echo "26g. discard: a parked poison note is discarded (the second discard is a 404), gone from the list, and its key flows again ..."
  for label in "service-b $B" "service-c $C"; do
    read -r label base <<< "$label"
    audit_failing "$base" true
    audit_note "$base" "k3-$run" poison-3
    id=$(await_letters "$base" 1 90 | cut -d' ' -f1)
    curl -fsS -X DELETE "$base/audit/dead-letters/$id" >/dev/null || fail "$label: discarding $id failed"
    n=$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "$base/audit/dead-letters/$id") || fail "$label: the second discard failed"
    [ "$n" = "404" ] || fail "$label: discarding $id again should be a 404, got $n"
    [ "$(audit_count "$base")" = "0" ] || fail "$label: the discarded letter is still counted"
    audit_failing "$base" false
    audit_note "$base" "k3-$run" after-discard
    await_handled "$base" "k3-$run" "after-discard" 30
    [ "$(audit_count "$base")" = "0" ] || fail "$label: the note after the discard became a letter"
    echo "   $label: $id discarded, k3-$run unblocked (after-discard handled, no letter)"
  done

  echo "26h. the hub's dead-letter count of both audit processors is back to 0 ..."
  for label in service-b service-c; do
    sum=$(await_processor "$label" audit "$label\.audit" 'RUNNING|FAILING' 0 90)
    echo "   $label audit: $sum"
  done
  audit_off
}

# P6b (ADR 0023): the hub as control plane for event processors, through its HTTP API
# ($HUB/api/v1/services/<service>/processors/audit/...) with the dashboard write token: kraken/infra/scripts/dev-hub-pki.sh
# writes it to <secrets>/hub-control/dashboard-write.token and every sandbox mode mounts it into both hubs
# (KRAKEN_HUB_DASHBOARD_WRITE_TOKEN_FILE). For the `audit` processor of service-b (Kotlin) and of service-c (Rust):
# 27: a write without a credential (or with a wrong one) is 401 on both hubs, with the dashboard token 403 (SECURE=1 only: the
# plain hubs have no dashboard token); leftover letters are cleared and the processor resumed. 27b: pause: the hub shows PAUSED and notes published
# meanwhile are not handled; resume: they are, and RUNNING again. 27c: notes under a key of this run are handled (and notified
# once), the processor is paused, then reset to earliest: the reset handler ran once (`/audit/resets`), the hub shows it
# PAUSED and replaying with no operation left (a finish keeps the pause, so nothing is replayed yet). 27d: resume: the notes
# of this run are handled again, the notifier (`@DisallowReplay` / `disallow_replay`) did not run again, and the replay ends
# (RUNNING, not replaying). The topic holds every note since the sandbox started, so only this run's keys are checked.
# 27e: dead letters through the hub: a poison note parked (the failure on), listed by the hub without a payload, retried
# (Failed while failing, Retried with the failure off, its key handled), the list empty; another one discarded (a second
# discard is a 404). SKIP_CONTROL=1 skips these steps, ONLY_CONTROL=1 runs only them.

# A call to <hub>'s control API for <service>'s audit processor, with the credential <cred>: `write` (the write token), `read`
# (the dashboard token, SECURE=1), `wrong` or `none`. Prints the body and then the HTTP code on a line of its own. A token goes
# through a curl config on a pipe, as the curl wrapper does, so it never shows in a process list.
control_call() { # <hub> <cred> <method> <service> <path> [json body]
  local url="$1/api/v1/services/$4/processors/audit/$5" token="" args=(-sS --max-time 200 -X "$3" -w '\n%{http_code}')
  [ "$SECURE" = "1" ] && args+=(--cacert "$SECRETS/ca.pem")
  case $2 in write) token=$WRITE_TOKEN ;; read) token=$DASHBOARD_TOKEN ;; wrong) token=wrong-write-token-0123456789 ;; esac
  [ -n "${6:-}" ] && args+=(-H 'content-type: application/json' -d "$6")
  if [ -n "$token" ]; then
    command curl "${args[@]}" -K <(printf 'header = "Authorization: Bearer %s"\n' "$token") "$url"
  else
    command curl "${args[@]}" "$url"
  fi
}

# control_call, expecting the HTTP code <code>; prints the body.
control_expect() { # <code> <hub> <cred> <method> <service> <path> [json body]
  local want=$1 reply code
  shift
  reply=$(control_call "$@") || fail "$3 $1 .../$4/processors/audit/$5 failed"
  code=${reply##*$'\n'}
  [ "$code" = "$want" ] || fail "$3 $1/api/v1/services/$4/processors/audit/$5 ($2 credential): expected $want, got $code: ${reply%$'\n'*}"
  printf '%s' "${reply%$'\n'*}"
}

# Waits until the hub shows <service>'s audit processor as "<state> <replaying> <operationId or ->" matching <pattern>.
await_control_state() { # <service> <pattern> <seconds>
  local start=$SECONDS st=""
  while [ $((SECONDS - start)) -lt "$3" ]; do
    st=$(curl -fsS "$HUB/api/v1/processors" 2>/dev/null | control_state "$1" audit || true)
    if printf '%s' "$st" | grep -Eq "^($2)$"; then printf '%s' "$st"; return 0; fi
    sleep 1
  done
  fail "$HUB/api/v1/processors: expected $1's audit processor as '$2' (state replaying operation) within ${3}s, got '${st:-not listed}'"
}

# The texts the audit processor's notifier at <base> handled for <key>, comma-separated (as audit_handled).
audit_notified() { # <base> <key>
  curl -fsS "$1/audit/notified" | { grep -o '{[^{}]*}' || true; } | grep "\"key\":\"$2\"" \
    | sed 's/.*"text":"\([^"]*\)".*/\1/' | paste -sd, - || true
}

# The resets the audit processor's reset handler at <base> recorded, one per line: "<operationId> <target>".
audit_resets() { # <base>
  curl -fsS "$1/audit/resets" | { grep -o '"operationId":"[^"]*","target":"[^"]*"' || true; } \
    | sed 's/"operationId":"\([^"]*\)","target":"\([^"]*\)"/\1 \2/'
}

# The hub's letters of <service>'s audit processor under <key> (as hub_letters), with the dashboard token in SECURE mode.
hub_letters_of() { # <service> <key>
  curl -fsS "$HUB/api/v1/services/$1/processors/audit/dead-letters?limit=100" | hub_letters | { grep " $2 " || true; }
}

# Waits until the hub lists exactly one letter of <service>'s audit processor under <key> and prints it.
await_hub_letter() { # <service> <key> <seconds>
  local start=$SECONDS got=""
  while [ $((SECONDS - start)) -lt "$3" ]; do
    got=$(hub_letters_of "$1" "$2")
    [ "$(printf '%s' "$got" | grep -c .)" = "1" ] && { printf '%s' "$got"; return 0; }
    sleep 1
  done
  fail "$HUB: expected one dead letter of $1's audit processor under key $2 within ${3}s, got '$got'"
}

# Leaves nothing behind when a step fails: both audit processors resumed, the failure off.
control_off() {
  trap - EXIT
  control_call "$HUB" write POST service-b resume >/dev/null 2>&1 || true
  control_call "$HUB" write POST service-c resume >/dev/null 2>&1 || true
  command curl -fsS -X POST "$B/audit/failing?on=false" >/dev/null 2>&1 || true
  command curl -fsS -X POST "$C/audit/failing?on=false" >/dev/null 2>&1 || true
}

control_steps() {
  local run file secrets label base reply code h st op n resets id key reason attempts
  run=$(date +%s)
  secrets=${SECRETS:-${SECRETS_DIR:-${KRAKEN_SECRETS_DIR:-$INFRA/.secrets}}}
  file=${WRITE_TOKEN_FILE:-$secrets/hub-control/dashboard-write.token}
  [ -s "$file" ] || fail "the control steps need the hubs' write token in '$file' (run kraken/infra/scripts/dev-hub-pki.sh, then recreate hub and hub-2; or set WRITE_TOKEN_FILE, or SKIP_CONTROL=1)"
  WRITE_TOKEN=$(cat "$file")
  trap control_off EXIT

  echo "27. a write without a credential or with a wrong one is 401 on both hubs, with the dashboard token 403; no letters left over ..."
  for label in "service-b $B" "service-c $C"; do
    read -r label base <<< "$label"
    for h in "$HUB" "$HUB2"; do
      reply=$(control_call "$h" none POST "$label" pause) || fail "POST $h .../$label/processors/audit/pause failed"
      code=${reply##*$'\n'}
      [ "$code" != "403" ] || fail "$h answers 403 to a write without a credential (${reply%$'\n'*}): the hubs have no write token; run kraken/infra/scripts/dev-hub-pki.sh and recreate hub and hub-2"
      [ "$code" = "401" ] || fail "$h: a pause of $label's audit without a credential should be 401, got $code"
      control_expect 401 "$h" wrong POST "$label" pause >/dev/null
      if [ "$SECURE" = "1" ]; then
        control_expect 403 "$h" read POST "$label" pause | grep -q 'read-only' || fail "$h: the dashboard token's 403 should say read-only"
      fi
    done
    audit_failing "$base" false
    curl -fsS -X POST "$base/audit/dead-letters/retry-all?processor=audit" >/dev/null || fail "retry-all of $label's leftover letters failed"
    for id in $(audit_letters "$base" | cut -d' ' -f1); do
      curl -fsS -X DELETE "$base/audit/dead-letters/$id" >/dev/null || fail "discarding the leftover letter $id of $label failed"
    done
    control_expect 200 "$HUB" write POST "$label" resume >/dev/null
    st=$(await_control_state "$label" '(RUNNING|FAILING) false -' 90)
    if [ "$SECURE" = "1" ]; then
      echo "   $label: none 401, wrong 401, dashboard token 403 (read-only) on both hubs; audit: $st"
    else
      echo "   $label: none 401, wrong 401 on both hubs (no dashboard token in this mode); audit: $st"
    fi
  done

  echo "27b. pause through the hub: PAUSED, notes published meanwhile are held; resume: they are handled ..."
  for label in "service-b $B" "service-c $C"; do
    read -r label base <<< "$label"
    reply=$(control_expect 200 "$HUB" write POST "$label" pause)
    [ "$(printf '%s' "$reply" | reply_field acknowledged)" -ge 1 ] && [ "$(printf '%s' "$reply" | reply_field failed)" = "0" ] \
      || fail "$label: the pause should be acknowledged by every instance, got: $reply"
    st=$(await_control_state "$label" 'PAUSED false -' 30)
    audit_note "$base" "p$run" held-1
    audit_note "$base" "p$run" held-2
    sleep 8
    [ -z "$(audit_handled "$base" "p$run")" ] || fail "$label: a note published while audit is paused was handled: $(audit_handled "$base" "p$run")"
    control_expect 200 "$HUB" write POST "$label" resume >/dev/null
    await_handled "$base" "p$run" "held-1,held-2" 30
    st=$(await_control_state "$label" '(RUNNING|FAILING) false -' 30)
    echo "   $label: paused (acknowledged $(printf '%s' "$reply" | reply_field acknowledged)), held-1 and held-2 held for 8 s, handled after the resume; audit: $st"
  done

  echo "27c. reset to earliest while paused: the reset handler runs once, the hub shows PAUSED and replaying, no operation left ..."
  for label in "service-b $B" "service-c $C"; do
    read -r label base <<< "$label"
    audit_note "$base" "r$run" replay-1
    audit_note "$base" "r$run" replay-2
    await_handled "$base" "r$run" "replay-1,replay-2" 30
    [ "$(audit_notified "$base" "r$run")" = "replay-1,replay-2" ] || fail "$label: the notifier should have handled replay-1,replay-2 once, got '$(audit_notified "$base" "r$run")'"
    resets=$(audit_resets "$base" | grep -c . || true)
    control_expect 200 "$HUB" write POST "$label" pause >/dev/null
    await_control_state "$label" 'PAUSED false -' 30 >/dev/null
    reply=$(control_expect 200 "$HUB" write POST "$label" reset '{"to":"earliest"}')
    op=$(printf '%s' "$reply" | reply_field operationId)
    [ "$(printf '%s' "$reply" | reply_field outcome)" = "ok" ] && [ "$(printf '%s' "$reply" | reply_field replay)" = "true" ] \
      && [ "$(printf '%s' "$reply" | reply_field resetHandlers)" = "1" ] && [ "$(printf '%s' "$reply" | reply_field markersDeleted)" -ge 2 ] \
      && [ -n "$op" ] || fail "$label: the reset to earliest should be ok, replay, one reset handler, at least 2 markers deleted, got: $reply"
    n=$(audit_resets "$base" | grep -c . || true)
    [ "$n" = "$((resets + 1))" ] && [ "$(audit_resets "$base" | tail -1)" = "$op earliest" ] \
      || fail "$label: the reset handler should have recorded '$op earliest' once, got $(audit_resets "$base" | tail -2 | paste -sd';' -)"
    st=$(await_control_state "$label" 'PAUSED true -' 30)
    [ "$(audit_handled "$base" "r$run")" = "replay-1,replay-2" ] || fail "$label: notes were replayed while audit is paused: $(audit_handled "$base" "r$run")"
    echo "   $label: operation $op ok, executed by $(printf '%s' "$reply" | reply_field executedBy), $(printf '%s' "$reply" | reply_field markersDeleted) markers deleted, reset handler once; audit: $st"
  done

  echo "27d. resume: this run's notes are handled again, the no-replay notifier is not, and the replay ends ..."
  for label in "service-b $B" "service-c $C"; do
    read -r label base <<< "$label"
    control_expect 200 "$HUB" write POST "$label" resume >/dev/null
    await_handled "$base" "r$run" "replay-1,replay-2,replay-1,replay-2" 180
    await_handled "$base" "p$run" "held-1,held-2,held-1,held-2" 30
    [ "$(audit_notified "$base" "r$run")" = "replay-1,replay-2" ] || fail "$label: the notifier ran again in the replay: '$(audit_notified "$base" "r$run")'"
    st=$(await_control_state "$label" '(RUNNING|FAILING) false -' 60)
    echo "   $label: r$run handled as replay-1,replay-2 twice, notified once; audit: $st"
  done

  echo "27e. dead letters through the hub: a parked note listed, retried (Failed, then Retried), the list empty; another discarded ..."
  for label in "service-b $B" "service-c $C"; do
    read -r label base <<< "$label"
    audit_failing "$base" true
    audit_note "$base" "d$run" poison-hub
    read -r id key reason attempts <<< "$(await_hub_letter "$label" "d$run" 90)"
    [ "$reason" = "HANDLER_FAILED" ] && [ "$attempts" -ge 1 ] || fail "$label: the hub should list d$run's letter HANDLER_FAILED, got '$id $key $reason $attempts'"
    curl -fsS "$HUB/api/v1/services/$label/processors/audit/dead-letters" | grep -q '"payload"' && fail "$label: the hub's letter list carries a payload"
    reply=$(control_expect 200 "$HUB" write POST "$label" "dead-letters/$id/retry")
    [ "$(printf '%s' "$reply" | reply_field result)" = "Failed" ] || fail "$label: retrying $id while failing should answer Failed, got: $reply"
    [ "$SECURE" = "1" ] && control_expect 403 "$HUB" read POST "$label" "dead-letters/$id/retry" >/dev/null
    audit_failing "$base" false
    reply=$(control_expect 200 "$HUB" write POST "$label" "dead-letters/$id/retry")
    [ "$(printf '%s' "$reply" | reply_field result)" = "Retried" ] || fail "$label: retrying $id with the failure off should answer Retried, got: $reply"
    await_handled "$base" "d$run" "poison-hub" 10
    reply=$(curl -fsS "$HUB/api/v1/services/$label/processors/audit/dead-letters")
    [ "$(printf '%s' "$reply" | reply_field total)" = "0" ] && [ -z "$(printf '%s' "$reply" | hub_letters)" ] \
      || fail "$label: the hub should list no letter after the retry, got: $reply"
    echo "   $label: $id listed by the hub (HANDLER_FAILED, $attempts attempts, no payload), retry Failed, then Retried; no letter left"
    audit_failing "$base" true
    audit_note "$base" "x$run" poison-discard
    id=$(await_hub_letter "$label" "x$run" 90 | cut -d' ' -f1)
    audit_failing "$base" false
    reply=$(control_expect 200 "$HUB" write DELETE "$label" "dead-letters/$id")
    [ "$(printf '%s' "$reply" | reply_field discarded)" = "true" ] || fail "$label: discarding $id should answer discarded, got: $reply"
    control_expect 404 "$HUB" write DELETE "$label" "dead-letters/$id" >/dev/null
    [ -z "$(hub_letters_of "$label" "x$run")" ] || fail "$label: the discarded letter $id is still listed"
    echo "   $label: $id discarded through the hub, a second discard 404"
  done
  control_off
}

# P5a (ADR 0018), only with SECURE=1 against the secure sandbox (kraken/infra/compose.secure.yaml). Step 21: a service
# with a token the ACL does not know, and one with a valid token that grants only another service name, are refused by
# both hubs and logged by the client as a registration rejection (ERROR once per hub), and never listed on /api/v1/topology; a
# request outside service-a's `send` patterns answers 403 AccessDeniedException. Step 21b: the dashboard API wants the
# dashboard token. SKIP_SECURE=1 skips them, ONLY_SECURE=1 runs only them.
INTRUDER=kraken-smoke-intruder

remove_intruder() { docker rm -f "$INTRUDER" >/dev/null 2>&1 || true; }

# How many lines of <hub container>'s log say it refused a register and match <pattern> (a run compares before and after,
# so it counts only its own).
refusals() { # <container> <pattern>
  { docker logs "$1" 2>&1 | grep 'register refused' | grep -c -- "$2"; } || true
}

# Starts the throwaway intruder (service-a's image, so the same library and defaults) claiming <service name> with
# <token> and TLS to both hubs; the Kafka consumer and the outbox are off so it touches neither Kafka nor MongoDB.
start_intruder() { # <service name> <token>
  local image
  image=$(docker inspect --format '{{.Config.Image}}' "${A_CONTAINER:-kraken-example-a}" 2>/dev/null) \
    || fail "container ${A_CONTAINER:-kraken-example-a} not found (set A_CONTAINER)"
  remove_intruder
  docker run -d --name "$INTRUDER" --network kraken \
    -e KRAKEN_SERVICE_NAME="$1" \
    -e KRAKEN_HUB_ADDRESS=static://hub:7000,hub-2:7000 \
    -e KRAKEN_KAFKA_BOOTSTRAP=kafka:19092 \
    -e KRAKEN_MONGODB_URI="mongodb://mongodb:27017/smoke-intruder?replicaSet=rs0" \
    -e SERVER_PORT=8080 \
    -e KRAKEN_MESSAGING_KAFKA_CONSUMER_ENABLED=false -e KRAKEN_MESSAGING_OUTBOX_ENABLED=false \
    -e KRAKEN_MESSAGING_HUB_TOKEN="$2" \
    -e KRAKEN_MESSAGING_HUB_TLS_ENABLED=true -e KRAKEN_MESSAGING_HUB_TLS_CA_FILE=/etc/kraken/hub-ca.pem \
    -v "$SECRETS/ca.pem:/etc/kraken/hub-ca.pem:ro" \
    "$image" >/dev/null || fail "could not start the intruder container from $image"
}

# Waits until both hubs' logs grew by at least <attempts> refusals matching <pattern> since <before hub> / <before hub-2>.
await_refusals() { # <pattern> <before hub> <before hub-2> <attempts>
  local i n1=0 n2=0
  for i in $(seq 1 120); do
    n1=$(refusals "${HUB_CONTAINER:-kraken-hub}" "$1"); n2=$(refusals "${HUB2_CONTAINER:-kraken-hub-2}" "$1")
    if [ "$n1" -ge $(( $2 + $4 )) ] && [ "$n2" -ge $(( $3 + $4 )) ]; then return 0; fi
    sleep 1
  done
  fail "the hubs did not both refuse the intruder ($1) $4 time(s) in 120s (hub: $n1, was $2; hub-2: $n2, was $3); intruder log: $(docker logs --tail 15 "$INTRUDER" 2>&1)"
}

not_on_topology() { # <service name>
  local h
  for h in "$HUB" "$HUB2"; do
    if curl -fsS "$h/api/v1/topology" | grep -q "\"serviceName\":\"$1\""; then fail "$1 is listed on $h/api/v1/topology although it was refused"; fi
  done
}

secure_steps() {
  local h1=${HUB_CONTAINER:-kraken-hub} h2=${HUB2_CONTAINER:-kraken-hub-2} b1 b2 wrong log errors code body h path
  command -v docker >/dev/null || fail "docker CLI needed for the security steps (or set SKIP_SECURE=1)"
  docker inspect "$h1" >/dev/null 2>&1 || fail "container $h1 not found (set HUB_CONTAINER, or SKIP_SECURE=1)"
  docker inspect "$h2" >/dev/null 2>&1 || fail "container $h2 not found (set HUB2_CONTAINER, or SKIP_SECURE=1)"
  trap remove_intruder EXIT

  echo "21. an unknown token is refused by both hubs: UNAUTHENTICATED, ERROR once per hub in the client's log, never on /api/v1/topology ..."
  wrong="smoke-wrong-token-$(date +%s)-$RANDOM"
  b1=$(refusals "$h1" smoke-intruder); b2=$(refusals "$h2" smoke-intruder)
  start_intruder smoke-intruder "$wrong"
  await_refusals smoke-intruder "$b1" "$b2" 2
  log=$(docker logs "$INTRUDER" 2>&1)
  errors=$(printf '%s\n' "$log" | grep -c 'Hub rejected the registration of smoke-intruder' || true)
  printf '%s\n' "$log" | grep 'Hub rejected the registration of smoke-intruder' | grep -q 'UNAUTHENTICATED' \
    || fail "the intruder did not log an UNAUTHENTICATED registration rejection: $(printf '%s\n' "$log" | tail -n 15)"
  [ "$errors" -ge 1 ] && [ "$errors" -le 2 ] \
    || fail "expected the rejection logged once per hub (1 or 2 lines) after several attempts, got $errors: $(printf '%s\n' "$log" | grep 'Hub rejected')"
  printf '%s\n' "$log" | grep 'Hub rejected the registration of smoke-intruder' | head -n 1 | sed 's/^/   client: /'
  docker logs "$h1" 2>&1 | grep 'register refused' | grep smoke-intruder | tail -n 1 | sed 's/^/   hub:    /'
  docker logs "$h2" 2>&1 | grep 'register refused' | grep smoke-intruder | tail -n 1 | sed 's/^/   hub-2:  /'
  echo "   both hubs refused it at least twice; the client logged the rejection $errors time(s) (ERROR once per hub, DEBUG after)"
  not_on_topology smoke-intruder
  echo "   smoke-intruder is absent from $HUB/api/v1/topology and $HUB2/api/v1/topology"

  echo "21. a valid token that grants another service name is refused: PERMISSION_DENIED when claiming service-a ..."
  b1=$(refusals "$h1" 'is not allowed to register as service service-a'); b2=$(refusals "$h2" 'is not allowed to register as service service-a')
  start_intruder service-a "$(cat "$SECRETS/smoke-intruder.token")"
  await_refusals 'is not allowed to register as service service-a' "$b1" "$b2" 1
  log=$(docker logs "$INTRUDER" 2>&1)
  printf '%s\n' "$log" | grep 'Hub rejected the registration of service-a' | grep -q 'PERMISSION_DENIED' \
    || fail "the intruder did not log a PERMISSION_DENIED registration rejection: $(printf '%s\n' "$log" | tail -n 15)"
  printf '%s\n' "$log" | grep 'Hub rejected the registration of service-a' | head -n 1 | sed 's/^/   client: /'
  docker logs "$h1" 2>&1 | grep 'register refused' | grep 'is not allowed to register as service service-a' | tail -n 1 | sed 's/^/   hub:    /'
  remove_intruder
  # The real service-a is untouched: the intruder claimed its name, the hubs registered nothing under it.
  curl -fsS "$A/greetings/Ada/count" >/dev/null || fail "service-a stopped answering while the intruder claimed its name"
  echo "   the real service-a still answers; $INTRUDER removed"

  echo "21. GET /forbidden-send: a type outside service-a's send patterns is answered 403 AccessDeniedException ..."
  body=$(mktemp)
  code=$(curl -s -o "$body" -w '%{http_code}' --max-time 15 "$A/forbidden-send" || true)
  echo "   HTTP $code: $(cat "$body")"
  [ "$code" = "403" ] || fail "expected 403 for a send outside the ACL, got $code: $(cat "$body")"
  grep -q 'AccessDeniedException' "$body" || fail "expected AccessDeniedException in the body, got: $(cat "$body")"
  rm -f "$body"

  echo "21b. the dashboard API wants the dashboard token: 401 without it (or with a wrong one), 200 with it, on both hubs ..."
  for h in "$HUB" "$HUB2"; do
    for path in /api/v1/topology /api/v1/types; do
      code=$(command curl -s -o /dev/null -w '%{http_code}' --cacert "$SECRETS/ca.pem" "$h$path" || true)
      [ "$code" = "401" ] || fail "GET $h$path without the token: expected 401, got $code"
      code=$(command curl -s -o /dev/null -w '%{http_code}' --cacert "$SECRETS/ca.pem" -H 'Authorization: Bearer wrong-dashboard-token' "$h$path" || true)
      [ "$code" = "401" ] || fail "GET $h$path with a wrong token: expected 401, got $code"
      code=$(curl -s -o /dev/null -w '%{http_code}' "$h$path" || true)
      [ "$code" = "200" ] || fail "GET $h$path with the token: expected 200, got $code"
      echo "   $h$path: 401 without, 401 with a wrong token, 200 with the token"
    done
  done
  # Plain text to the TLS port never gets an answer.
  code=$(command curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://${HUB#https://}/api/v1/topology" || true)
  [ "$code" = "000" ] || fail "plain-text HTTP to the TLS port ${HUB#https://} answered $code, expected the connection to fail"
  echo "   plain-text HTTP to the TLS port gets no answer"
  trap - EXIT
}

# P5b (ADR 0019), only with SECURE=1 JWT=1 against the JWT sandbox (kraken/infra/compose.jwt.yaml): service-b (Kotlin) and
# service-c (Rust) register with a Keycloak access token (a dev identity provider, tokens that live 30 s), service-a with its
# static token. Step 22: both hubs list service-b and service-c with authKind jwt and an expiresAt, and service-a with
# authKind token. Step 22b: within ~40 s the expiresAt of both services advances on both hubs while the instance, and the
# stream it registered on (connectedSinceMs), stay the same: the token was renewed and handed to the live stream as
# ClientMessage.credentials, not by a reconnect (service-c's log has no new "registered with the hub" line either); the hubs
# never logged "credentials expired". SKIP_JWT=1 skips them, ONLY_JWT=1 runs only them.
# The hub's counters (kraken_hub_jwt_validations_total{result="ok"}, ...) go out over OTLP only, which the examples closure
# does not run a collector for; authKind jwt on the topology is the hub saying it validated the token.

# <service>'s first instance on <hub url>'s topology, from "instanceId" on, as one line (empty while it is not listed).
# instance_fields stops at "tags":{} (it splits at '{'), before authKind and expiresAt: cut at each instanceId instead and take
# the chunk that follows the service's name.
session_fields() { # <hub url> <service>
  curl -fsS "$1/api/v1/topology" 2>/dev/null | sed 's/"instanceId"/\n"instanceId"/g' \
    | awk -v s="\"serviceName\":\"$2\"" 'index($0, s) { f = 1; next } f && /^"instanceId"/ { print; exit }' || true
}

# "<instanceId> <connectedSinceMs> <authKind> <expiresAt or ->" of <service>'s first instance on <hub url>'s topology.
jwt_session() { # <hub url> <service>
  local fields id since kind exp
  fields=$(session_fields "$1" "$2")
  id=$(printf '%s' "$fields" | sed -n 's/.*"instanceId":"\([^"]*\)".*/\1/p')
  since=$(printf '%s' "$fields" | sed -n 's/.*"connectedSinceMs":\([0-9]*\).*/\1/p')
  kind=$(printf '%s' "$fields" | sed -n 's/.*"authKind":"\([^"]*\)".*/\1/p')
  exp=$(printf '%s' "$fields" | sed -n 's/.*"expiresAt":\([0-9]*\).*/\1/p')
  echo "${id:--} ${since:--} ${kind:--} ${exp:--}"
}

# Waits until <service> shows authKind <kind> on <hub url> (a service that just reconnected needs a moment) and prints its session.
await_auth_kind() { # <hub url> <service> <kind>
  local i sess
  for i in $(seq 1 60); do
    sess=$(jwt_session "$1" "$2")
    if [ "$(echo "$sess" | cut -d' ' -f3)" = "$3" ]; then echo "$sess"; return 0; fi
    sleep 1
  done
  fail "$2 does not show authKind $3 on $1/api/v1/topology within 60s (last: '$sess')"
}

# How many lines of <container>'s log contain <pattern>; with <since> (an RFC 3339 time) only the lines logged from then on.
log_count() { # <container> <pattern> [<since>]
  { docker logs ${3:+--since "$3"} "$1" 2>&1 | grep -c -- "$2"; } || true
}

jwt_steps() {
  local h1=${HUB_CONTAINER:-kraken-hub} h2=${HUB2_CONTAINER:-kraken-hub-2} c=${C_CONTAINER:-kraken-example-c}
  local h svc sess id since kind exp i k advanced registered_before registered_after expired jwt_started
  local ids=() sinces=() exps=()    # per (hub, service), in the order of the loops below (no associative arrays: macOS bash 3.2)
  command -v docker >/dev/null || fail "docker CLI needed for the JWT steps (or set SKIP_JWT=1)"
  docker inspect "$h1" >/dev/null 2>&1 || fail "container $h1 not found (set HUB_CONTAINER, or SKIP_JWT=1)"
  docker inspect "$h2" >/dev/null 2>&1 || fail "container $h2 not found (set HUB2_CONTAINER, or SKIP_JWT=1)"
  docker inspect "$c" >/dev/null 2>&1 || fail "container $c not found (set C_CONTAINER, or SKIP_JWT=1)"
  # The hubs' logs keep the history of earlier runs: "credentials expired" is only looked for from here on.
  jwt_started=$(date -u +%Y-%m-%dT%H:%M:%SZ)

  echo "22. both hubs show service-b (Kotlin) and service-c (Rust) with authKind jwt and an expiresAt, service-a with token ..."
  for h in "$HUB" "$HUB2"; do
    for svc in service-b service-c; do
      sess=$(await_auth_kind "$h" "$svc" jwt)
      read -r id since kind exp <<< "$sess"
      [ "$exp" != "-" ] || fail "$svc shows authKind jwt on $h but no expiresAt: $sess"
      echo "   $h $svc: authKind $kind, expiresAt $exp ($(( exp / 1000 - $(date +%s) )) s from now)"
      ids+=("$id"); sinces+=("$since"); exps+=("$exp")
    done
    sess=$(await_auth_kind "$h" service-a token)
    read -r id since kind exp <<< "$sess"
    [ "$exp" = "-" ] || fail "service-a (static token) must have no expiresAt on $h, got $exp"
    echo "   $h service-a: authKind $kind, no expiresAt"
  done
  docker logs "$h1" 2>&1 | grep 'register refused' | grep -E 'jwks not loaded' | head -n 1 | sed 's/^/   hub (before it had the keys): /' || true

  echo "22b. within ~40 s the tokens are renewed on the live streams: expiresAt advances, same instance, same stream, no reconnect ..."
  registered_before=$(log_count "$c" 'registered with the hub')
  for i in $(seq 1 45); do
    advanced=1; k=0
    for h in "$HUB" "$HUB2"; do
      for svc in service-b service-c; do
        read -r id since kind exp <<< "$(jwt_session "$h" "$svc")"
        if [ "$exp" = "-" ] || [ "$exp" -le "${exps[$k]}" ]; then advanced=0; fi
        k=$((k + 1))
      done
    done
    [ "$advanced" = "1" ] && break
    sleep 1
  done
  [ "$advanced" = "1" ] || fail "expiresAt did not advance on both hubs for both services in 45s (last: $h $svc $id $since $kind $exp)"
  k=0
  for h in "$HUB" "$HUB2"; do
    for svc in service-b service-c; do
      read -r id since kind exp <<< "$(jwt_session "$h" "$svc")"
      [ "$id" = "${ids[$k]}" ] && [ "$since" = "${sinces[$k]}" ] \
        || fail "$svc reconnected to $h while its token was renewed (instance ${ids[$k]} -> $id, connectedSinceMs ${sinces[$k]} -> $since)"
      echo "   $h $svc: expiresAt ${exps[$k]} -> $exp (+$(( (exp - ${exps[$k]}) / 1000 )) s), same instance $id, connectedSinceMs $since unchanged"
      k=$((k + 1))
    done
  done
  registered_after=$(log_count "$c" 'registered with the hub')
  [ "$registered_before" = "$registered_after" ] \
    || fail "service-c logged $((registered_after - registered_before)) new 'registered with the hub' line(s) while its token was renewed"
  echo "   service-c's log has no new 'registered with the hub' line ($registered_after, as before)"
  expired=$(( $(log_count "$h1" 'credentials expired' "$jwt_started") + $(log_count "$h2" 'credentials expired' "$jwt_started") ))
  [ "$expired" -eq 0 ] || fail "the hubs logged 'credentials expired' $expired time(s) since $jwt_started"
  echo "   neither hub logged 'credentials expired' since $jwt_started"
}

# P5c (ADR 0020), only with SECURE=1 MTLS=1 against the mTLS sandbox (kraken/infra/compose.mtls.yaml): service-a (Kotlin) and
# service-c (Rust) register with a client certificate (a dev client CA, exactly one URI SAN kraken://service/<name>), service-b
# with its static token (the hubs run with KRAKEN_HUB_TLS_CLIENT_AUTH=optional). Step 23: both hubs list service-a and
# service-c with authKind certificate, the fingerprint of the certificate in the service's mounted directory and an
# expiresAt, and service-b with token. Step 23b: the certificate of service-c (then of service-a) is replaced on disk by a new
# one (new key; dev-hub-pki.sh --rotate) while a steady trickle of calls goes through service-a (alternating service-b and
# service-c as the target): within ~60 s both hubs show the new fingerprint (each instance first waits a random 0 to 30 s, so replicas do not roll together) for the same instance (the libraries roll the
# connections over one hub at a time, no restart) and no call failed. SKIP_MTLS=1 skips them, ONLY_MTLS=1 runs only them.

# The first 8 hex characters of the SHA-256 of the DER encoding of the certificate in <pem>: what the hub shows.
cert_fp_of() { # <pem>
  openssl x509 -in "$1" -noout -fingerprint -sha256 | sed 's/.*=//; s/://g' | tr 'A-F' 'a-f' | cut -c1-8
}

# "<instanceId> <connectedSinceMs> <authKind> <certFingerprint or ->" of <service>'s first instance on <hub url>'s topology.
cert_session() { # <hub url> <service>
  local fields id since kind fp
  fields=$(session_fields "$1" "$2")
  id=$(printf '%s' "$fields" | sed -n 's/.*"instanceId":"\([^"]*\)".*/\1/p')
  since=$(printf '%s' "$fields" | sed -n 's/.*"connectedSinceMs":\([0-9]*\).*/\1/p')
  kind=$(printf '%s' "$fields" | sed -n 's/.*"authKind":"\([^"]*\)".*/\1/p')
  fp=$(printf '%s' "$fields" | sed -n 's/.*"certFingerprint":"\([^"]*\)".*/\1/p')
  echo "${id:--} ${since:--} ${kind:--} ${fp:--}"
}

# Waits until <service> shows authKind certificate and <fingerprint> on <hub url>; prints its session.
await_cert() { # <hub url> <service> <fingerprint> <seconds>
  local i sess
  for i in $(seq 1 "$4"); do
    sess=$(cert_session "$1" "$2")
    if [ "$(echo "$sess" | cut -d' ' -f3)" = "certificate" ] && [ "$(echo "$sess" | cut -d' ' -f4)" = "$3" ]; then
      echo "$sess"; return 0
    fi
    sleep 1
  done
  fail "$2 does not show authKind certificate with fingerprint $3 on $1/api/v1/topology within $4 s (last: '$sess')"
}

mtls_steps() {
  local h1=${HUB_CONTAINER:-kraken-hub} h2=${HUB2_CONTAINER:-kraken-hub-2}
  local h svc sess id since kind fp fp2 exp k n started waited codes bad total mtls_started stop out loop_pid
  local ids=() sinces=()    # per hub, in the order of the loops below (no associative arrays: macOS bash 3.2)
  command -v docker >/dev/null || fail "docker CLI needed for the certificate steps (or set SKIP_MTLS=1)"
  command -v openssl >/dev/null || fail "openssl needed for the certificate steps (or set SKIP_MTLS=1)"
  [ -f "$INFRA/scripts/dev-hub-pki.sh" ] || fail "kraken/infra/scripts/dev-hub-pki.sh not found (set INFRA_DIR)"
  docker inspect "$h1" >/dev/null 2>&1 || fail "container $h1 not found (set HUB_CONTAINER, or SKIP_MTLS=1)"
  docker inspect "$h2" >/dev/null 2>&1 || fail "container $h2 not found (set HUB2_CONTAINER, or SKIP_MTLS=1)"
  mtls_started=$(date -u +%Y-%m-%dT%H:%M:%SZ)

  echo "23. both hubs show service-a (Kotlin) and service-c (Rust) with authKind certificate, the fingerprint of their certificate and an expiresAt, service-b with token ..."
  for h in "$HUB" "$HUB2"; do
    for svc in service-a service-c; do
      fp=$(cert_fp_of "$SECRETS/mtls/$svc/client.pem")
      sess=$(await_cert "$h" "$svc" "$fp" 60)
      read -r id since kind fp <<< "$sess"
      exp=$(session_fields "$h" "$svc" | sed -n 's/.*"expiresAt":\([0-9]*\).*/\1/p')
      [ -n "$exp" ] || fail "$svc shows authKind certificate on $h but no expiresAt"
      echo "   $h $svc: authKind $kind, certFingerprint $fp, expiresAt $exp ($(( (exp / 1000 - $(date +%s)) / 86400 )) days from now)"
    done
    sess=$(await_auth_kind "$h" service-b token)
    read -r id since kind exp <<< "$sess"
    if session_fields "$h" service-b | grep -q certFingerprint; then fail "service-b (static token) must show no certFingerprint on $h"; fi
    echo "   $h service-b: authKind $kind, no certFingerprint"
  done

  echo "23b. the certificate is replaced on disk (new key) while calls flow: both hubs show the new fingerprint, same instance, no call fails ..."
  for svc in service-c service-a; do
    ids=(); sinces=(); k=0
    for h in "$HUB" "$HUB2"; do
      read -r id since kind fp <<< "$(cert_session "$h" "$svc")"
      ids+=("$id"); sinces+=("$since")
    done
    stop=$(mktemp -u); out=$(mktemp)
    started=$(now_ms)
    ha_loop "$out" "$stop" "$started" &
    loop_pid=$!
    sleep 2
    fp=$(cert_fp_of "$SECRETS/mtls/$svc/client.pem")
    bash "$INFRA/scripts/dev-hub-pki.sh" --dir "$SECRETS" --rotate "$svc" | sed 's#^#   #; s#(in .*; #(#'
    n=$(cert_fp_of "$SECRETS/mtls/$svc/client.pem")
    [ "$n" != "$fp" ] || { : > "$stop"; wait "$loop_pid" || true; fail "the rotation did not change $svc's certificate ($fp)"; }
    started=$(now_ms)
    for h in "$HUB" "$HUB2"; do
      sess=$(await_cert "$h" "$svc" "$n" 90) || { : > "$stop"; wait "$loop_pid" || true; exit 1; }
      waited=$(( ($(now_ms) - started) / 1000 ))
      read -r id since kind fp2 <<< "$sess"
      [ "$id" = "${ids[$k]}" ] || { : > "$stop"; wait "$loop_pid" || true; fail "$svc changed instance on $h during the rotation (${ids[$k]} -> $id)"; }
      [ "$since" != "${sinces[$k]}" ] || { : > "$stop"; wait "$loop_pid" || true; fail "$svc shows the new fingerprint on $h but did not reconnect (connectedSinceMs $since unchanged)"; }
      echo "   $h $svc: certFingerprint $fp -> $n after ${waited} s, same instance $id, connectedSinceMs ${sinces[$k]} -> $since"
      k=$((k + 1))
    done
    sleep 2
    : > "$stop"; wait "$loop_pid" || true
    codes=$(cut -d' ' -f1 "$out"); total=$(printf '%s\n' "$codes" | grep -c . || true)
    bad=$(printf '%s\n' "$codes" | grep -vc '^200$' || true)
    rm -f "$stop" "$out"
    [ "$total" -ge 10 ] || fail "only $total calls went through while $svc's certificate was rotated"
    [ "$bad" -eq 0 ] || fail "$bad of $total calls failed while $svc's certificate was rotated: $(printf '%s\n' "$codes" | sort | uniq -c | tr '\n' ' ')"
    echo "   $total calls through service-a (service-b and service-c as targets) during the rotation of $svc, every one answered 200"
  done
  k=$(( $(log_count "$h1" 'certificate expired' "$mtls_started") + $(log_count "$h2" 'certificate expired' "$mtls_started") ))
  [ "$k" -eq 0 ] || fail "the hubs logged 'certificate expired' $k time(s) since $mtls_started"
  echo "   neither hub logged 'certificate expired' since $mtls_started"
}

echo "1. waiting for service-a at $A ..."
for i in $(seq 1 90); do
  if curl -fsS "$A/actuator/health" 2>/dev/null | grep -q '"UP"'; then break; fi
  [ "$i" -eq 90 ] && fail "service-a did not become healthy in 90s"
  sleep 1
done

echo "1b. waiting for service-b to own examples.CountGreetings on the hub ($HUB) ..."
for i in $(seq 1 90); do
  TOPO=$(curl -fsS "$HUB/api/v1/topology" 2>/dev/null || true)
  if echo "$TOPO" | grep -q '"type":"examples.CountGreetings","kind":"QUERY","liveInstances":[1-9][0-9]*,[^}]*"ownerServices":\["service-b"\]'; then break; fi
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

if [ "${ONLY_STREAM:-0}" = "1" ]; then
  stream_steps
  echo "SMOKE OK (streaming-query steps only)"
  exit 0
fi

if [ "${ONLY_SUBSCRIBE:-0}" = "1" ]; then
  subscribe_steps
  echo "SMOKE OK (subscription-query steps only)"
  exit 0
fi

if [ "${ONLY_POSTCARD:-0}" = "1" ]; then
  postcard_steps
  echo "SMOKE OK (postcard steps only)"
  exit 0
fi

if [ "${ONLY_BANNER:-0}" = "1" ]; then
  banner_steps
  echo "SMOKE OK (banner steps only)"
  exit 0
fi

if [ "${ONLY_AUDIT:-0}" = "1" ]; then
  audit_steps
  echo "SMOKE OK (audit steps only)"
  exit 0
fi

if [ "${ONLY_CONTROL:-0}" = "1" ]; then
  control_steps
  echo "SMOKE OK (processor-control steps only)"
  exit 0
fi

if [ "${ONLY_SECURE:-0}" = "1" ]; then
  [ "$SECURE" = "1" ] || fail "ONLY_SECURE=1 needs SECURE=1"
  secure_steps
  echo "SMOKE OK (security steps only)"
  exit 0
fi

if [ "${ONLY_JWT:-0}" = "1" ]; then
  [ "$JWT" = "1" ] || fail "ONLY_JWT=1 needs JWT=1 (and SECURE=1)"
  jwt_steps
  echo "SMOKE OK (JWT steps only)"
  exit 0
fi

if [ "${ONLY_MTLS:-0}" = "1" ]; then
  [ "$MTLS" = "1" ] || fail "ONLY_MTLS=1 needs MTLS=1 (and SECURE=1)"
  mtls_steps
  echo "SMOKE OK (certificate steps only)"
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
TOPOLOGY=$(curl -fsS "$HUB/api/v1/topology") || fail "GET $HUB/api/v1/topology failed"
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
  TOPO=$(curl -fsS "$HUB/api/v1/topology" 2>/dev/null || true)
  if echo "$TOPO" | grep -q '"type":"examples.Reverse","kind":"QUERY","liveInstances":[1-9][0-9]*,[^}]*"ownerServices":\["service-c"\]' \
     && echo "$TOPO" | grep -q '"type":"examples.Shout","kind":"COMMAND","liveInstances":[1-9][0-9]*,[^}]*"ownerServices":\["service-c"\]'; then break; fi
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
[ "${SKIP_STREAM:-0}" = "1" ] || stream_steps
[ "${SKIP_SUBSCRIBE:-0}" = "1" ] || subscribe_steps
[ "${SKIP_POSTCARD:-0}" = "1" ] || postcard_steps
[ "${SKIP_BANNER:-0}" = "1" ] || banner_steps
[ "${SKIP_AUDIT:-0}" = "1" ] || audit_steps
[ "${SKIP_CONTROL:-0}" = "1" ] || control_steps
if [ "$SECURE" = "1" ] && [ "${SKIP_SECURE:-0}" != "1" ]; then secure_steps; fi
if [ "$JWT" = "1" ] && [ "${SKIP_JWT:-0}" != "1" ]; then jwt_steps; fi
if [ "$MTLS" = "1" ] && [ "${SKIP_MTLS:-0}" != "1" ]; then mtls_steps; fi

echo "SMOKE OK"
