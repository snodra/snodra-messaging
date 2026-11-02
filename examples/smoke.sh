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
#        SECURE=1 runs against the secure sandbox (kraken/infra/compose.secure.yaml, secrets from
#        kraken/infra/scripts/dev-hub-pki.sh): the hub calls are https with the dev CA and carry the dashboard token, every
#        other step runs unchanged, and the security steps 21 and 21b are added; SKIP_SECURE=1 skips those two, ONLY_SECURE=1
#        runs only them; SECRETS_DIR names the secrets directory, default KRAKEN_SECRETS_DIR or INFRA_DIR/.secrets)
set -euo pipefail

SECURE=${SECURE:-0}
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

# SECURE=1: the hubs speak TLS with the dev CA and want the dashboard token on /topology and /api/v1/*, and the
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
  # /topology -> "<totals.inFlight> <sum over every instance of inFlightAsHandler + inFlightAsSender>".
  in_flight() {
    jq -r '"\(.totals.inFlight // 0) \([.services[].instances[] | (.inFlightAsHandler // 0) + (.inFlightAsSender // 0)] | add // 0)"' 2>/dev/null || true
  }
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
      t=$(curl -fsS "$h/topology" 2>/dev/null | in_flight)
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
      t=$(curl -fsS "$h/topology" 2>/dev/null | in_flight)
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

# P5a (ADR 0018), only with SECURE=1 against the secure sandbox (kraken/infra/compose.secure.yaml). Step 21: a service
# with a token the ACL does not know, and one with a valid token that grants only another service name, are refused by
# both hubs and logged by the client as a registration rejection (ERROR once per hub), and never listed on /topology; a
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
    if curl -fsS "$h/topology" | grep -q "\"serviceName\":\"$1\""; then fail "$1 is listed on $h/topology although it was refused"; fi
  done
}

secure_steps() {
  local h1=${HUB_CONTAINER:-kraken-hub} h2=${HUB2_CONTAINER:-kraken-hub-2} b1 b2 wrong log errors code body h path
  command -v docker >/dev/null || fail "docker CLI needed for the security steps (or set SKIP_SECURE=1)"
  docker inspect "$h1" >/dev/null 2>&1 || fail "container $h1 not found (set HUB_CONTAINER, or SKIP_SECURE=1)"
  docker inspect "$h2" >/dev/null 2>&1 || fail "container $h2 not found (set HUB2_CONTAINER, or SKIP_SECURE=1)"
  trap remove_intruder EXIT

  echo "21. an unknown token is refused by both hubs: UNAUTHENTICATED, ERROR once per hub in the client's log, never on /topology ..."
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
  echo "   smoke-intruder is absent from $HUB/topology and $HUB2/topology"

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
    for path in /topology /api/v1/types; do
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
  code=$(command curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://${HUB#https://}/topology" || true)
  [ "$code" = "000" ] || fail "plain-text HTTP to the TLS port ${HUB#https://} answered $code, expected the connection to fail"
  echo "   plain-text HTTP to the TLS port gets no answer"
  trap - EXIT
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

if [ "${ONLY_SECURE:-0}" = "1" ]; then
  [ "$SECURE" = "1" ] || fail "ONLY_SECURE=1 needs SECURE=1"
  secure_steps
  echo "SMOKE OK (security steps only)"
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
[ "${SKIP_STREAM:-0}" = "1" ] || stream_steps
[ "${SKIP_SUBSCRIBE:-0}" = "1" ] || subscribe_steps
if [ "$SECURE" = "1" ] && [ "${SKIP_SECURE:-0}" != "1" ]; then secure_steps; fi

echo "SMOKE OK"
