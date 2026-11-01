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
   a point query (one of them answers) and as a scatter-gather (both answer).

Everything is anonymous, nothing is persisted by the apps (MongoDB is only there because the starter's outbox and
idempotent consumer need it), and the modules are never published. They build against the starter as a project
dependency, so they always exercise the current library. Wire classes live in `examples-api`.

| Module | Role | Host port |
|---|---|---|
| `examples/examples-api` | `SendGreeting`, `GreetingAccepted`, `GreetingDelivered`, `CountGreetings`, `GreetingCount`; the texts types; the shared `Inventory` / `InventoryLevel` | – |
| `examples/service-a` | REST caller + event listener | 8091 |
| `examples/service-b` | owner of the greetings command and query, co-owner of the shared `Inventory`; logs the text events | 8092 (actuator only) |
| `kraken-messaging-rs/examples/service-c` | Rust: owner of `Shout` and `Reverse`, co-owner of the shared `Inventory`, publishes the text events, logs `GreetingDelivered` | – (no HTTP) |

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

# who is registered for what (service-c owns examples.Shout and examples.Reverse; examples.Inventory is shared)
curl -s localhost:7001/topology
curl -s localhost:7001/api/v1/types   # examples.Inventory: "shared":true, "owners":["service-b","service-c"]
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
```

Add `service-c` to the `logs -f` command to see the `C:` lines.

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
```

## Experiments

- **Nobody owns the command (Rust side).** `docker compose ... stop service-c`, then `POST /shouts`: the same 503
  `NoHandlerException`; `start service-c` and it recovers within seconds.
- **Nobody owns the command.** `docker compose ... stop service-b`, then post a greeting:
  `503 {"error":"NoHandlerException","message":"No handler registered for 'examples.SendGreeting'"}`.
  `docker compose ... start service-b` and the next post is a `200` once B has re-registered (a few seconds).
- **Hub restart.** `docker compose ... restart hub`; the apps reconnect and re-register on their own, and meanwhile
  every call goes through `hub-2`. Only a call in flight on the stopped hub can fail (502,
  `HubConnectionLostException`). Stop both hubs to see the single-hub behaviour: 502 until a hub is back.
- **Counts are in memory.** Restarting service-b resets `count` to 0; the events service-a already received stay in
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
| the hub does not offer scatter-gather (a P3 or older hub) | 501 | `{"error":"UnsupportedByHubException", ...}` |
| any other messaging failure | 502 | `{"error":"<exception>", ...}` |
| blank `name` | 400 | RFC 9457 problem detail: `{"type":"about:blank","title":"Bad Request","status":400,"detail":"name must not be blank",...}` |

## Tear down

```bash
docker compose -f compose.yaml -f compose.local.yaml --profile examples down        # containers
docker compose -f compose.yaml -f compose.local.yaml --profile examples down -v     # also Kafka and Mongo volumes
```
