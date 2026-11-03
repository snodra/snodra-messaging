# Examples: two services through the hub

A disposable sandbox that shows cross-service messaging on the real `kraken-hub` image and Kafka, driven by `curl`:

1. **service-a** sends the command `examples.SendGreeting` over the hub;
2. **service-b** handles it, counts the greeting in memory and publishes the event `examples.GreetingDelivered`
   (through its outbox, then Kafka);
3. **service-a** reacts to the event and remembers it;
4. **service-a** sends the query `examples.CountGreetings` over the hub; **service-b** answers from its counts;
5. **service-a** sends the command `examples.Shout` and the query `examples.Reverse` to **service-c**, a Rust
   service built on `kraken-messaging-rs`; C publishes `examples.TextShouted` / `examples.TextReversed` while
   handling them, and A and B log those events; C also logs B's `GreetingDelivered`;
6. **service-b** and **service-c** both own the shared query `examples.Inventory` (ADR 0017): **service-a** sends it as
   a point query (one of them answers) and as a scatter-gather (both answer);
7. **service-b** and **service-c** both own the shared streaming query `examples.Rows` (ADR 0017, P4b): **service-a**
   opens a stream (one instance of either streams the rows under A's credits), collects it and returns the rows;
8. **service-b** and **service-c** both serve the shared subscription query `examples.WatchCounter` (ADR 0017, P4c) with an
   in-memory counter per name: **service-a** subscribes (the initial result is one counter's current value, the updates
   are the changes of both), collects the initial result and N updates, cancels and returns them. The updates come from
   `POST /counters/{name}/increment` on service-b and from the command `examples.IncrementCounter` that service-a sends
   to service-c (which has no HTTP); both push through a `QueryUpdateEmitter`.

Everything is anonymous, nothing is persisted by the apps (MongoDB is only there because the starter's outbox and
idempotent consumer need it), and the modules are never published. They build against the starter as a project
dependency, so they always exercise the current library. Wire classes live in `examples-api`.

| Module | Role | Host port |
|---|---|---|
| `examples/examples-api` | `SendGreeting`, `GreetingAccepted`, `GreetingDelivered`, `CountGreetings`, `GreetingCount`; the texts types; the shared `Inventory` / `InventoryLevel`; the streaming `Rows` / `Row`; the subscription `WatchCounter` / `Counter` / `CounterChanged`, the command `IncrementCounter` and the shared query `OpenSubscriptions` / `SubscriptionCount` | – |
| `examples/service-a` | REST caller + event listener; `GET /forbidden-send` sends a type outside its ACL on purpose ([Secure sandbox](#secure-sandbox)) | 8091 |
| `examples/service-b` | owner of the greetings command and query, co-owner of the shared `Inventory`, the streaming `Rows`, the subscription `WatchCounter` and `OpenSubscriptions`; logs the text events | 8092 (`POST /counters/{name}/increment` and actuator) |
| `kraken-messaging-rs/examples/service-c` | Rust: owner of `Shout`, `Reverse` and `IncrementCounter`, co-owner of the shared `Inventory`, `Rows`, `WatchCounter` and `OpenSubscriptions`, publishes the text events, logs `GreetingDelivered` | – (no HTTP) |

## Prerequisites

- Docker (Rancher Desktop or Docker Desktop) with Compose v2.
- The sibling checkouts `../kraken` (umbrella, provides `infra/compose.yaml` and `infra/compose.local.yaml`),
  `../kraken-hub` and `../kraken-messaging-rs` next to this repo, as described in the umbrella's `docs/07-local-development.md`.

## Start

From `kraken/infra`:

```bash
docker compose -f compose.yaml -f compose.local.yaml --profile examples up --build -d service-a service-b service-c
```

This builds `kraken-hub:dev`, `kraken-examples-service-a:dev`, `kraken-examples-service-b:dev` and
`kraken-examples-service-c:dev` from source (the first build takes several minutes, Rust compiles librdkafka; later
ones are cached) and starts only what the three apps need: both hubs (`hub`, `hub-2`), Kafka,
the topic init job and MongoDB. Keycloak, RustFS and Grafana stay down. The hub's gRPC port is published on host
**7002** (macOS AirPlay owns 7000) and its topology view and dashboard on **7001**; `hub-2` uses **7012** and
**7011**. Every example connects to both hubs (`KRAKEN_HUB_ADDRESS=static://hub:7000,hub-2:7000`, ADR 0016): it
registers on each, sends unkeyed requests round-robin over them and keeps working when one stops.

If the repos are not siblings of `kraken`, point the build contexts at them:
`KRAKEN_HUB_DIR=... KRAKEN_MESSAGING_DIR=... KRAKEN_MESSAGING_RS_DIR=... docker compose ...`.

## Drive it

```bash
# 1 + 2 + 3: command over the hub, event back over Kafka
curl -s -X POST localhost:8091/greetings -H 'content-type: application/json' -d '{"name":"Ada"}'
# {"id":"<uuid>","name":"Ada"}

# 3: what service-a received so far
curl -s localhost:8091/greetings/delivered
# [{"id":"<uuid>","name":"Ada","at":"2026-10-30T00:49:45.227152891Z"}]

# 4: query over the hub
curl -s localhost:8091/greetings/Ada/count
# {"name":"Ada","count":1}

# 5: command and query to the Rust service, events back to A (and B)
curl -s -X POST localhost:8091/shouts -H 'content-type: application/json' -d '{"text":"abc"}'
# {"text":"ABC"}
curl -s localhost:8091/reverse/abc
# {"text":"cba"}
curl -s localhost:8091/texts/delivered
# [{"kind":"shouted","text":"ABC","at":"..."},{"kind":"reversed","text":"cba","at":"..."}]

# 6: the shared query examples.Inventory: a point query (service-b or service-c answers, alternating) ...
curl -s localhost:8091/inventory
# {"service":"service-c","instance":"<container id>","item":"widget","count":3,"answered":1}
# ... and a scatter-gather (every instance of both answers; ?item=...&maxResults=n to stop after n answers)
curl -s localhost:8091/scatter/inventory
# {"parts":[{"service":"service-b","instance":"<instance id>","result":{"service":"service-b",...,"count":7,...},"error":null},
#           {"service":"service-c","instance":"<instance id>","result":{...,"count":3,...},"error":null}],"failed":0,"timedOut":0}

# 7: the shared streaming query examples.Rows: one instance of service-b or service-c streams count rows under A's
# credits (credit = the window, refilled at half; maxResults = the hub ends the stream after n rows; take = A cancels
# after n rows; failAt = the handler fails on that row, after streaming the rows before it: 502 RemoteHandlerException)
curl -s 'localhost:8091/stream/rows?count=5&credit=2'
# {"rows":[{"service":"service-c","instance":"<container id>","n":1},...,{"service":"service-c","instance":"<container id>","n":5}],"count":5,"service":"service-c"}
curl -s 'localhost:8091/stream/rows?count=1000&take=50'      # 50 rows, the handler is cancelled
curl -s 'localhost:8091/stream/rows?count=10&failAt=3'       # {"error":"RemoteHandlerException","message":"...Rows failed at row 3"}

# 8: the shared subscription query examples.WatchCounter: A subscribes (the initial result is the current value of one
# owner's counter, 0 for a new name), collects the initial result and `updates` changes of either owner, or what arrived
# within `timeoutMs`, and cancels. Run it, then within the timeout bump the counters: service-b through its own endpoint,
# service-c through a command that A sends it (`service` is the addressee; only service-c owns IncrementCounter)
curl -s 'localhost:8091/subscribe/counter?name=x&updates=2&timeoutMs=20000' &
curl -s -X POST localhost:8092/counters/x/increment      # {"service":"service-b","instance":"<container id>","name":"x","value":1}
curl -s -X POST localhost:8091/counters/x/increment      # {"service":"service-c",...,"value":1}
# {"initial":{"service":"service-c",...,"name":"x","value":0},"updates":[{"service":"service-b",...,"value":1},{"service":"service-c",...,"value":1}]}
curl -s localhost:8091/subscribe/open                    # how many subscriptions each owner instance serves now (a scatter-gather of examples.OpenSubscriptions)
# {"instances":[{"service":"service-b","instance":"<container id>","open":0},{"service":"service-c",...,"open":0}],"failed":0}

# who is registered for what (service-c owns examples.Shout and examples.Reverse; examples.Inventory and examples.Rows are shared)
curl -s localhost:7001/topology
curl -s localhost:7001/api/v1/types   # examples.Inventory, examples.Rows, examples.WatchCounter: "shared":true, "owners":["service-b","service-c"]
```

The logs tell the same story, one `INFO` line per step:

```bash
docker compose -f compose.yaml -f compose.local.yaml --profile examples logs -f service-a service-b
```

```
service-b | B: handled SendGreeting name=Ada id=<uuid> correlationId=<id>
service-a | A: sent SendGreeting name=Ada -> accepted id=<uuid>
service-a | A: received GreetingDelivered id=<uuid> name=Ada at=...
service-b | B: answered CountGreetings name=Ada count=1
service-a | A: asked CountGreetings name=Ada -> count=1
service-c | C: received GreetingDelivered id=<uuid> name=Ada
service-c | C: handled Shout text=abc -> ABC correlationId=<id>
service-a | A: sent Shout text=abc -> ABC
service-a | A: received TextShouted text=ABC at=...
service-b | B: received TextShouted text=ABC at=...
service-c | C: answered Reverse text=abc -> cba
service-a | A: asked Reverse text=abc -> cba
service-c | C: answered Inventory item=widget answered=1
service-a | A: asked Inventory item=widget -> service-c (<container id>)
service-b | B: answered Inventory item=widget answered=1
service-c | C: answered Inventory item=widget answered=2
service-a | A: scatter-gathered Inventory item=widget -> 2 parts (0 failed, 0 timed out) from [service-b, service-c]
service-b | B: streaming Rows count=5 failAt=0
service-b | B: streamed Rows count=5
service-a | A: streamed Rows count=5 credit=2 maxResults=0 take=0 -> 5 rows from service-b (<container id>)
service-c | C: streaming Rows count=5 failAt=0
service-a | A: streamed Rows count=5 credit=2 maxResults=0 take=0 -> 5 rows from service-c (<container id>)
service-c | C: subscribed WatchCounter name=x value=0
service-b | B: incremented counter name=x -> 1
service-c | C: incremented counter name=x -> 1
service-a | A: subscribed WatchCounter name=x -> initial service-c=0 (<container id>), 2 of 2 updates [service-b=1, service-c=1]
```

Add `service-c` to the `logs -f` command to see the `C:` lines. (`subscribed WatchCounter` is logged only by the instance that answered the initial result; the other owner just registers the subscription.)

`examples/smoke.sh` runs the whole sequence and checks every step (it waits for service-a and for service-b to own its types on the hub, posts two greetings,
waits for both events, asserts the count grew by two, asserts both services are on the topology, then waits for
service-c, shouts and reverses a per-run unique text and waits for both text events to reach A). It asserts only
what A exposes over HTTP: B's and C's log lines and the order in which the text events arrive are not asserted;
read them in the logs. Last, it freezes service-b with `docker pause` during a `CountGreetings` call and expects
`503 HandlerDisconnectedException` after about 10 s: the hub evicts a v2 session after 10 s without heartbeats (it
took 30 s before protocol v2), and the sandbox hub gives `examples.CountGreetings` a 20 s per-type timeout
(`KRAKEN_HUB_TIMEOUT_OVERRIDES` in `compose.local.yaml`) so the call outlives the eviction instead of ending in a 5 s
`TIMEOUT`. It then waits for service-b to answer again. Finally it checks flow control (P1). The sandbox runs every
service with the libraries' default concurrency; the one-permit demo is the opt-in override
`kraken/infra/compose.flow-demo.yaml` (service-b: `KRAKEN_MESSAGING_HUB_MAX_CONCURRENT_HANDLERS=1` and a slowed
`CountGreetings`, `examples.count-delay` / env `EXAMPLES_COUNT_DELAY=200ms`; service-c (Rust):
`KRAKEN_MAX_CONCURRENT_HANDLERS=1`). The smoke recreates service-b and service-c with it (step 11b), then step 12
sends 20 parallel counts: all must answer 200, one at a time (at least 4 s; about 4.5 s, since a one-permit instance
gets each permit back at once), with the topology showing requests queued for service-b while they wait. Step 13
sends 10 parallel shouts from Kotlin service-a to Rust service-c: all must answer 200. After each burst the topology
must show the instance `flowControlled` with its permit back and nothing queued. Step 14 recreates both with the
default concurrency again (also when a step failed). `SERVICE_B_MAX_CONCURRENT_HANDLERS`,
`SERVICE_C_MAX_CONCURRENT_HANDLERS` and `EXAMPLES_COUNT_DELAY` tune the demo (the smoke expects its defaults);
`INFRA_DIR` points the smoke at `kraken/infra` when the checkouts are not siblings. With two hubs each gets
`ceil(1 / 2) = 1` permit of the one-permit instances, but an instance still runs one handler at a time, so step 12
stays serialised (step 5b looks for its graph edge on either hub, since requests alternate between them).

The two-hub steps (P3, ADR 0016) come last: step 15 waits up to 15 s until both hubs' `/topology` list the same
service-a, service-b and service-c instances, each reporting both hub ids in `connectedHubIds`; step 16 checks that
hub-2's `/api/v1/version` lists both dashboards as peers; step 17 runs a loop of calls through service-a (alternating
`CountGreetings` to service-b and `Reverse` to service-c), stops `kraken-hub-2` after 2 s and keeps calling for 4 s:
at most `HA_MAX_FAILURES` (5) calls may fail (in flight on the stopped hub), none may take more than 6 s, and every
call started a second after the hub was gone must succeed. It then starts the hub again and waits up to 15 s for every
instance to be back on it (also on failure the hub is started again). They need `jq` or `python3`; `SKIP_HA=1` skips
them, `ONLY_HA=1` runs only them, `HUB2` and `HUB2_CONTAINER` name the second hub.

The scatter-gather steps (P4a, ADR 0017) close the run: step 18 waits until both hubs' `/api/v1/types` list
`examples.Inventory` as `shared` with the owners `service-b` and `service-c`; step 18b calls `GET /scatter/inventory`
and expects parts from both services with `failed` and `timedOut` 0; step 18c calls `GET /inventory` six times and
expects both services among the answerers (one round-robin pool over both owners). `SKIP_SCATTER=1` skips them,
`ONLY_SCATTER=1` runs only them.

The streaming-query steps (P4b, ADR 0017) come last: step 19 calls `GET /stream/rows?count=1000&credit=8` (up to
twelve times, until both owners streamed once) and expects 1000 rows numbered 1..1000 in order, all from one
service and instance; step 19b adds `take=50` (service-a cancels the collection after 50 rows, which cancels the
handler through the hub) and expects 50 rows and both hubs' `/topology` in-flight counts (`totals.inFlight` and every
instance's `inFlightAsHandler` + `inFlightAsSender`) back at 0 within 10 s; step 19c calls
`GET /stream/rows?count=10&failAt=3` and expects `502 {"error":"RemoteHandlerException","message":"...Rows failed at
row 3"}` (the two rows streamed before the failure are not reported: the endpoint returns the error only).
`SKIP_STREAM=1` skips them, `ONLY_STREAM=1` runs only them.

The subscription-query steps (P4c, ADR 0017) come last: step 20a waits until both hubs' `/api/v1/types` list
`examples.WatchCounter` as `shared` with the owners `service-b` and `service-c` and records how many subscriptions each owner
serves (`GET /subscribe/open`, which scatter-gathers `examples.OpenSubscriptions`); step 20 opens
`GET /subscribe/counter?name=<unique>&updates=4&timeoutMs=10000` in the background, waits until both owners hold the
subscription, bumps the counter twice through service-b (`POST $B/counters/<name>/increment`, `B` = `http://localhost:8092`) and
twice through service-c (`POST /counters/<name>/increment` on service-a) and expects the initial result (value 0 to 2: an increment may land on the answering service before it reads its counter, and is then also delivered as an update) and four
updates, `1,2` from each service in order; step 20b checks that no subscription is left behind once the request returned,
**black-box** (the hub has no metrics endpoint): both owners' open-subscription counts are back to what they were before
step 20 (the proof that the hub's `Cancel` reached every target; the hubs' `/topology` in-flight counts, 0, are only a
secondary sanity check, as the hub does not count a subscription as in flight after its initial part), and a follow-up subscription (initial value 2 from either owner)
is cancelled and leaves the counts at the baseline again; step 20c subscribes to a name nobody incremented and expects the
initial result with value 0. `SKIP_SUBSCRIBE=1` skips them, `ONLY_SUBSCRIBE=1` runs only them.

To keep the flow-control demo running by hand:

```bash
docker compose -f compose.yaml -f compose.local.yaml -f compose.flow-demo.yaml --profile examples \
  up -d --no-deps --no-build service-b service-c   # from kraken/infra
```

```bash
bash examples/smoke.sh          # from kraken-messaging; prints SMOKE OK
SKIP_PAUSE=1 bash examples/smoke.sh   # without the docker pause step
SKIP_FLOW=1 bash examples/smoke.sh    # without the flow-control steps (they recreate service-b and service-c)
ONLY_FLOW=1 bash examples/smoke.sh    # only the flow-control steps (needs the docker CLI)
ONLY_PAUSE=1 bash examples/smoke.sh   # only the docker pause step (needs the docker CLI)
SKIP_HA=1 bash examples/smoke.sh      # without the two-hub steps (they stop and start kraken-hub-2)
ONLY_HA=1 bash examples/smoke.sh      # only the two-hub steps (needs the docker CLI and jq or python3)
SKIP_SCATTER=1 bash examples/smoke.sh # without the scatter-gather steps
ONLY_SCATTER=1 bash examples/smoke.sh # only the scatter-gather steps (needs jq or python3)
SKIP_STREAM=1 bash examples/smoke.sh  # without the streaming-query steps
ONLY_STREAM=1 bash examples/smoke.sh  # only the streaming-query steps (needs jq or python3)
SKIP_SUBSCRIBE=1 bash examples/smoke.sh # without the subscription-query steps
ONLY_SUBSCRIBE=1 bash examples/smoke.sh # only the subscription-query steps (needs jq or python3)
SECURE=1 bash examples/smoke.sh        # against the secure sandbox (see "Secure sandbox"): every step plus 21 and 21b
SECURE=1 SKIP_SECURE=1 bash examples/smoke.sh  # the secure sandbox without steps 21 and 21b
SECURE=1 ONLY_SECURE=1 bash examples/smoke.sh  # only steps 21 and 21b (needs the docker CLI)
SECURE=1 JWT=1 bash examples/smoke.sh  # against the JWT sandbox (see "JWT sandbox"): every step plus 21, 21b, 22 and 22b
SECURE=1 JWT=1 SKIP_JWT=1 bash examples/smoke.sh  # the JWT sandbox without steps 22 and 22b
SECURE=1 JWT=1 ONLY_JWT=1 bash examples/smoke.sh  # only steps 22 and 22b (needs the docker CLI)
SECURE=1 MTLS=1 bash examples/smoke.sh # against the mTLS sandbox (see "mTLS sandbox"): every step plus 21, 21b, 23 and 23b
SECURE=1 MTLS=1 SKIP_MTLS=1 bash examples/smoke.sh  # the mTLS sandbox without steps 23 and 23b
SECURE=1 MTLS=1 ONLY_MTLS=1 bash examples/smoke.sh  # only steps 23 and 23b (needs the docker CLI and openssl)
```

## Secure sandbox

The same sandbox with hub access control, TLS and a dashboard token (P5a, umbrella ADR 0018): the overlay
`kraken/infra/compose.secure.yaml`, with secrets generated by `kraken/infra/scripts/dev-hub-pki.sh` into the git-ignored
`kraken/infra/.secrets/`. Nothing secret is committed; the script is idempotent (`--force` regenerates).

```bash
# from kraken/infra
scripts/dev-hub-pki.sh        # dev CA, hub certificate (SANs hub, hub-2, localhost, 127.0.0.1), one token per service, the ACL
docker compose -f compose.yaml -f compose.local.yaml -f compose.secure.yaml --profile examples \
  up -d --force-recreate hub hub-2 service-a service-b service-c
# from kraken-messaging
SECURE=1 INFRA_DIR=../kraken/infra bash examples/smoke.sh     # every step, plus 21 and 21b; ends with SMOKE OK
# from kraken/infra: back to the plain stack
docker compose -f compose.yaml -f compose.local.yaml --profile examples \
  up -d --force-recreate hub hub-2 service-a service-b service-c
docker compose -f compose.yaml -f compose.local.yaml -f compose.secure.yaml --profile examples down   # or tear everything down
```

Both hubs run `enforce` (`KRAKEN_HUB_AUTH_MODE=permissive` in the environment of the `up` shows what would be refused instead).
The ACL gives each example exactly what it registers and sends: service-a handles nothing and sends `SendGreeting`,
`CountGreetings`, `Shout`, `Reverse`, `Inventory`, `Rows`, `WatchCounter`, `IncrementCounter` and `OpenSubscriptions`;
service-b handles `SendGreeting`, `CountGreetings`, `Inventory`, `Rows`, `WatchCounter` and `OpenSubscriptions` and sends
nothing; service-c (Rust) handles `Shout`, `Reverse`, `IncrementCounter` and the same shared types and sends nothing; a
`smoke-intruder` token may register as `smoke-intruder` and do nothing. The services read their token from a file
(`KRAKEN_MESSAGING_HUB_TOKEN_FILE` for the Kotlin ones, `KRAKEN_HUB_TOKEN_FILE` for service-c) and trust the dev CA
(`KRAKEN_MESSAGING_HUB_TLS_ENABLED` / `KRAKEN_MESSAGING_HUB_TLS_CA_FILE`, `KRAKEN_HUB_TLS` / `KRAKEN_HUB_TLS_CA`). The hub ports are
TLS-only, so by hand:

```bash
curl --cacert ../kraken/infra/.secrets/ca.pem -H "Authorization: Bearer $(cat ../kraken/infra/.secrets/dashboard.token)" \
  https://localhost:7001/topology
curl -s localhost:8091/forbidden-send      # 403 {"error":"AccessDeniedException","message":"service service-a is not allowed to send type examples.ForbiddenProbe"}
```

`GET /forbidden-send` (service-a) sends `examples.ForbiddenProbe`, a query type nobody handles and service-a's ACL deliberately
does not list; the hub answers `PERMISSION_DENIED` before routing and the advice maps `AccessDeniedException` to 403. Against
the plain sandbox the same call is the usual 503 `NoHandlerException`.

`SECURE=1 bash examples/smoke.sh` makes the hub calls (`$HUB`, `$HUB2`) https with the dev CA and the dashboard token, and runs every
existing step unchanged (the flow and HA steps recreate containers with the overlay as well). It adds, last:

- **Step 21**: a throwaway container `kraken-smoke-intruder` from the service-a image (removed afterwards, also on failure), with
  the Kafka consumer and the outbox off. First with a wrong token under the name `smoke-intruder`: both hubs log
  `register refused: unknown token` at least twice, the client logs `Hub rejected the registration of smoke-intruder ...
  UNAUTHENTICATED unknown token` once per hub (the registration rejection: ERROR once, DEBUG after) and `/topology` of both hubs never
  lists it. Then with the `smoke-intruder` token claiming `service-a`: `PERMISSION_DENIED` (`token ... is not allowed to register as
  service service-a`), and the real service-a keeps answering. Last `GET /forbidden-send` must answer 403 `AccessDeniedException`.
- **Step 21b**: `/topology` and `/api/v1/types` on both hubs answer 401 without the dashboard token (and with a wrong one) and 200 with
  it, and plain-text HTTP to the TLS port gets no answer.

`SECRETS_DIR` (default `KRAKEN_SECRETS_DIR`, else `INFRA_DIR/.secrets`) names the secrets directory; `HUB_CONTAINER`, `HUB2_CONTAINER` and
`A_CONTAINER` name the containers the steps read logs and the image from. `SKIP_SECURE=1` skips steps 21 and 21b, `ONLY_SECURE=1` runs
only them (both need `SECURE=1`).

## JWT sandbox

The secure sandbox with service-b (Kotlin) and service-c (Rust) authenticating by JWT (P5b, umbrella ADR 0019): the overlay
`kraken/infra/compose.jwt.yaml` on top of the secure one, with a throwaway Keycloak (`hub-idp`, container `kraken-hub-idp`, its own
realm `kraken-hub-dev`, tokens that live 30 s). service-a keeps its static token, so both credential kinds run side by side. The
identity provider, the realm and the client secrets are dev only; the secrets come from `dev-hub-pki.sh` into the git-ignored
`kraken/infra/.secrets/` (rerun it with `--force` once if that directory predates P5b).

```bash
# from kraken/infra
scripts/dev-hub-pki.sh
docker compose -f compose.yaml -f compose.local.yaml -f compose.secure.yaml -f compose.jwt.yaml --profile examples \
  up -d --force-recreate hub hub-2 hub-idp service-a service-b service-c
# from kraken-messaging
SECURE=1 JWT=1 INFRA_DIR=../kraken/infra bash examples/smoke.sh     # every step, plus 21, 21b, 22 and 22b; ends with SMOKE OK
# from kraken/infra: back to the secure stack, and stop the identity provider
docker compose -f compose.yaml -f compose.local.yaml -f compose.secure.yaml --profile examples \
  up -d --force-recreate hub hub-2 service-a service-b service-c
docker rm -f kraken-hub-idp
```

The hubs start without waiting for the provider (`KRAKEN_HUB_JWT_ISSUER=http://hub-idp:8080/realms/kraken-hub-dev`,
`KRAKEN_HUB_JWT_AUDIENCE=kraken-hub`): until their first key fetch works a JWT register is answered `UNAVAILABLE` "jwks not loaded yet"
and the clients retry, so the stack converges by itself. The services set `KRAKEN_HUB_OAUTH2_TOKEN_URI` (the provider's token endpoint)
and `KRAKEN_HUB_OAUTH2_CLIENT_SECRET_FILE`; the client id defaults to the service name, which the hub checks against the token's `azp`.
Their static token variables and mounts are removed, because a static token together with OAuth2 is a startup error. The ACL has a
`[[service]]` table for service-b and service-c with the same `handle` / `send` as their `[[token]]` entries.

`SECURE=1 JWT=1 bash examples/smoke.sh` (JWT needs SECURE) runs every step above unchanged (the flow steps recreate service-b and
service-c with the JWT overlay as well) and adds, last:

- **Step 22**: on both hubs' `/topology`, service-b and service-c have `authKind` `jwt` and an `expiresAt` (epoch milliseconds) and
  service-a has `token` and none. (The hub's `kraken_hub_jwt_validations_total` goes out over OTLP only, which the examples closure
  runs no collector for; `authKind` `jwt` is the hub saying it validated the token.)
- **Step 22b**: within ~45 s `expiresAt` advances on both hubs for both services (a new 30 s token, renewed at about 80 % of its life)
  while the instance id and `connectedSinceMs` stay the same, so the token reached the live stream as `ClientMessage.credentials` and
  there was no reconnect. service-c's log has no new `registered with the hub` line and neither hub logged `credentials expired`.

`C_CONTAINER` names service-c's container; `HUB_CONTAINER`, `HUB2_CONTAINER` as for the secure steps. `SKIP_JWT=1` skips steps 22 and 22b,
`ONLY_JWT=1` runs only them (both need `JWT=1`).

## mTLS sandbox

The secure sandbox with service-a (Kotlin) and service-c (Rust) authenticating with a client certificate (P5c, umbrella ADR 0020): the
overlay `kraken/infra/compose.mtls.yaml` on top of the secure one. service-b keeps its static token (the hubs run
`KRAKEN_HUB_TLS_CLIENT_AUTH=optional`), so both kinds run side by side. It is not combined with the JWT overlay. The client CA, the
certificates (EC P-256, 30 days, exactly one URI SAN `kraken://service/<name>`) and their keys come from `dev-hub-pki.sh` into the
git-ignored `kraken/infra/.secrets/` (rerun it with `--force` once if that directory predates P5c).

```bash
# from kraken/infra
scripts/dev-hub-pki.sh
docker compose -f compose.yaml -f compose.local.yaml -f compose.secure.yaml -f compose.mtls.yaml --profile examples \
  up -d --force-recreate hub hub-2 service-a service-b service-c
# from kraken-messaging
SECURE=1 MTLS=1 INFRA_DIR=../kraken/infra bash examples/smoke.sh    # every step, plus 21, 21b, 23 and 23b; ends with SMOKE OK
# from kraken/infra: back to the secure stack
docker compose -f compose.yaml -f compose.local.yaml -f compose.secure.yaml --profile examples \
  up -d --force-recreate hub hub-2 service-a service-b service-c
```

service-a sets `KRAKEN_MESSAGING_HUB_TLS_CLIENT_CERT_FILE` / `KRAKEN_MESSAGING_HUB_TLS_CLIENT_KEY_FILE` (`kraken.messaging.hub.tls.client-cert-file`
/ `client-key-file`), service-c `KRAKEN_HUB_TLS_CLIENT_CERT` / `KRAKEN_HUB_TLS_CLIENT_KEY`. Their token variables are emptied and their
token mounts removed (a token together with a certificate is a startup error). Each mounts its own **directory**
(`.secrets/mtls/<service>/`, `client.pem` and `client.key`) and the hub CA, read-only: a rotation replaces the files by rename, which a
single-file bind mount would not show. The ACL has a `[[service]]` table for service-a and service-c with the rights of their `[[token]]` entries.

`SECURE=1 MTLS=1 bash examples/smoke.sh` (MTLS needs SECURE and excludes JWT) runs every step above unchanged (the flow steps recreate
service-b and service-c with the mTLS overlay as well) and adds, last:

- **Step 23**: on both hubs' `/topology`, service-a and service-c have `authKind` `certificate`, a `certFingerprint` equal to the
  first 8 hex characters of the SHA-256 of the certificate in their directory, and an `expiresAt` (the certificate's `notAfter`);
  service-b has `token` and no fingerprint.
- **Step 23b**: `dev-hub-pki.sh --rotate service-c` replaces service-c's certificate and key on disk while a steady trickle of calls goes
  through service-a (alternating service-b and service-c as targets). Within ~30 s both hubs show the new `certFingerprint` for the same
  instance id (the library rolled its hub connections over one hub at a time, so `connectedSinceMs` is new), every call answered
  200, and neither hub logged `certificate expired`. The same is then done for service-a (Kotlin). The script cannot sign on demand
  (the client CA key is deleted), so it hands out one of ten spare pairs per service (`.secrets/mtls-spare/`); each smoke run uses
  one per service, and when they are gone `dev-hub-pki.sh --force` issues more.

`A_CONTAINER`, `HUB_CONTAINER` and `HUB2_CONTAINER` name the containers as for the secure steps. `SKIP_MTLS=1` skips steps 23 and 23b,
`ONLY_MTLS=1` runs only them (both need `MTLS=1`).

## Experiments

- **Nobody owns the command (Rust side).** `docker compose ... stop service-c`, then `POST /shouts`: the same 503
  `NoHandlerException`; `start service-c` and it recovers within seconds.
- **Nobody owns the command.** `docker compose ... stop service-b`, then post a greeting:
  `503 {"error":"NoHandlerException","message":"No handler registered for 'examples.SendGreeting'"}`.
  `docker compose ... start service-b` and the next post is a `200` once B has re-registered (a few seconds).
- **Hub restart.** `docker compose ... restart hub`; the apps reconnect and re-register on their own, and meanwhile
  every call goes through `hub-2`. Only a call in flight on the stopped hub can fail (502,
  `HubConnectionLostException`). Stop both hubs to see the single-hub behaviour: 502 until a hub is back.
- **Counts are in memory.** Restarting service-b resets `count` to 0 (and its `WatchCounter` counters); the events service-a already received stay in
  its own memory until it restarts.
- **Run one side from the IDE.** `./gradlew :examples:service-a:bootRun` (or `service-b`) works against the compose
  infrastructure: the defaults in `application.yaml` point at the published host ports (hub `localhost:7002`,
  Kafka `localhost:9092`, MongoDB `localhost:27017`). Stop the containerised copy first or both will handle the same
  types (the hub rejects a second owner of a type from a different service name, but the same name just adds an
  instance).

## HTTP errors on service-a

| Hub outcome | HTTP | Body |
|---|---|---|
| no handler registered for the type | 503 | `{"error":"NoHandlerException","message":"..."}` |
| owner disconnected mid-request | 503 | `{"error":"HandlerDisconnectedException", ...}` |
| no reply within the request timeout | 504 | `{"error":"MessageTimeoutException", ...}` |
| the hub's access control refused the request: the type is outside the token's `send` patterns (`GET /forbidden-send` does it on purpose) | 403 | `{"error":"AccessDeniedException","message":"..."}` |
| the hub does not offer scatter-gather, streaming or subscriptions (a P3 / P4a / P4b or older hub) | 501 | `{"error":"UnsupportedByHubException", ...}` |
| the handler's flow failed (`/stream/rows?failAt=n`) | 502 | `{"error":"RemoteHandlerException","message":"...Rows failed at row n"}` (the rows before it are not reported) |
| the hub ended a stream or a subscription for outliving its maximum duration | 502 | `{"error":"StreamCancelledException", ...}` |
| an owner at its subscription limit, or the subscriber's buffer overflowed | 503 | `{"error":"HandlerBusyException", ...}` |
| any other messaging failure | 502 | `{"error":"<exception>", ...}` |
| blank `name` | 400 | RFC 9457 problem detail: `{"type":"about:blank","title":"Bad Request","status":400,"detail":"name must not be blank",...}` |

## Tear down

(Add `-f compose.secure.yaml` to the commands below when the secure overlay is up; it only needs `infra/.secrets/` to exist.)

```bash
docker compose -f compose.yaml -f compose.local.yaml --profile examples down        # containers
docker compose -f compose.yaml -f compose.local.yaml --profile examples down -v     # also Kafka and Mongo volumes
```
