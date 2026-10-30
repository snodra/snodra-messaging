# Examples: two services through the hub

A disposable sandbox that shows cross-service messaging on the real `kraken-hub` image and Kafka, driven by `curl`:

1. **service-a** sends the command `examples.SendGreeting` over the hub;
2. **service-b** handles it, counts the greeting in memory and publishes the event `examples.GreetingDelivered`
   (through its outbox, then Kafka);
3. **service-a** reacts to the event and remembers it;
4. **service-a** sends the query `examples.CountGreetings` over the hub; **service-b** answers from its counts.

Everything is anonymous, nothing is persisted by the apps (MongoDB is only there because the starter's outbox and
idempotent consumer need it), and the modules are never published. They build against the starter as a project
dependency, so they always exercise the current library. Wire classes live in `examples-api`.

| Module | Role | Host port |
|---|---|---|
| `examples/examples-api` | `SendGreeting`, `GreetingAccepted`, `GreetingDelivered`, `CountGreetings`, `GreetingCount` | – |
| `examples/service-a` | REST caller + event listener | 8091 |
| `examples/service-b` | owner of the command and the query | 8092 (actuator only) |

## Prerequisites

- Docker (Rancher Desktop or Docker Desktop) with Compose v2.
- The sibling checkouts `../kraken` (umbrella, provides `infra/compose.yaml` and `infra/compose.local.yaml`) and
  `../kraken-hub` next to this repo, as described in the umbrella's `docs/07-local-development.md`.

## Start

From `kraken/infra`:

```bash
docker compose -f compose.yaml -f compose.local.yaml --profile examples up --build -d service-a service-b
```

This builds `kraken-hub:dev`, `kraken-examples-service-a:dev` and `kraken-examples-service-b:dev` from source
(the first build takes a few minutes, later ones are cached) and starts only what the two apps need: hub, Kafka,
the topic init job and MongoDB. Keycloak, RustFS and Grafana stay down. The hub's gRPC port is published on host
**7002** (macOS AirPlay owns 7000) and its topology view on **7001**.

If the repos are not siblings of `kraken`, point the build contexts at them:
`KRAKEN_HUB_DIR=/path/to/kraken-hub KRAKEN_MESSAGING_DIR=/path/to/kraken-messaging docker compose ...`.

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

# who is registered for what
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
```

`examples/smoke.sh` runs the whole sequence and checks every step (it waits for service-a, posts two greetings,
waits for both events, asserts the count grew by two, and asserts both services are on the topology):

```bash
bash examples/smoke.sh          # from kraken-messaging; prints SMOKE OK
```

## Experiments

- **Nobody owns the command.** `docker compose ... stop service-b`, then post a greeting:
  `503 {"error":"NoHandlerException","message":"No handler registered for 'examples.SendGreeting'"}`.
  `docker compose ... start service-b` and the next post is a `200` once B has re-registered (a few seconds).
- **Hub restart.** `docker compose ... restart hub`; both apps reconnect and re-register on their own, the next
  post succeeds.
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
| blank `name` | 400 | Spring's problem detail |

## Tear down

```bash
docker compose -f compose.yaml -f compose.local.yaml --profile examples down        # containers
docker compose -f compose.yaml -f compose.local.yaml --profile examples down -v     # also Kafka and Mongo volumes
```
