# Examples: two services through the hub

A disposable sandbox that shows cross-service messaging on the real `kraken-hub` image and Kafka, driven by `curl`:

1. **service-a** sends the command `examples.SendGreeting` over the hub;
2. **service-b** handles it, counts the greeting in memory and publishes the event `examples.GreetingDelivered`
   (through its outbox, then Kafka);
3. **service-a** reacts to the event and remembers it;
4. **service-a** sends the query `examples.CountGreetings` over the hub; **service-b** answers from its counts;
5. **service-a** sends the command `examples.Shout` and the query `examples.Reverse` to **service-c**, a Rust
   service built on `kraken-messaging-rs`; C publishes `examples.TextShouted` / `examples.TextReversed` while
   handling them, and A and B log those events; C also logs B's `GreetingDelivered`.

Everything is anonymous, nothing is persisted by the apps (MongoDB is only there because the starter's outbox and
idempotent consumer need it), and the modules are never published. They build against the starter as a project
dependency, so they always exercise the current library. Wire classes live in `examples-api`.

| Module | Role | Host port |
|---|---|---|
| `examples/examples-api` | `SendGreeting`, `GreetingAccepted`, `GreetingDelivered`, `CountGreetings`, `GreetingCount` | – |
| `examples/service-a` | REST caller + event listener | 8091 |
| `examples/service-b` | owner of the greetings command and query; logs the text events | 8092 (actuator only) |
| `kraken-messaging-rs/examples/service-c` | Rust: owner of `Shout` and `Reverse`, publishes the text events, logs `GreetingDelivered` | – (no HTTP) |

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
ones are cached) and starts only what the three apps need: hub, Kafka,
the topic init job and MongoDB. Keycloak, RustFS and Grafana stay down. The hub's gRPC port is published on host
**7002** (macOS AirPlay owns 7000) and its topology view on **7001**.

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

# who is registered for what (service-c owns examples.Shout and examples.Reverse)
curl -s localhost:7001/topology
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
`INFRA_DIR` points the smoke at `kraken/infra` when the checkouts are not siblings. To keep the demo running by hand:

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
```

## Experiments

- **Nobody owns the command (Rust side).** `docker compose ... stop service-c`, then `POST /shouts`: the same 503
  `NoHandlerException`; `start service-c` and it recovers within seconds.
- **Nobody owns the command.** `docker compose ... stop service-b`, then post a greeting:
  `503 {"error":"NoHandlerException","message":"No handler registered for 'examples.SendGreeting'"}`.
  `docker compose ... start service-b` and the next post is a `200` once B has re-registered (a few seconds).
- **Hub restart.** `docker compose ... restart hub`; both apps reconnect and re-register on their own. A post sent
  in the first seconds can still get a 503 (B not yet re-registered) or 502 (A still reconnecting); after a few
  seconds posts succeed again.
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
| any other messaging failure | 502 | `{"error":"<exception>", ...}` |
| blank `name` | 400 | RFC 9457 problem detail: `{"type":"about:blank","title":"Bad Request","status":400,"detail":"name must not be blank",...}` |

## Tear down

```bash
docker compose -f compose.yaml -f compose.local.yaml --profile examples down        # containers
docker compose -f compose.yaml -f compose.local.yaml --profile examples down -v     # also Kafka and Mongo volumes
```
