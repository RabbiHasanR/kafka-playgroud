# Integration tests

These tests talk to a real Kafka broker — no mocked Kafka client anywhere under this
tree, unlike `tests/unit/`.

Start the broker first:

```
docker compose up -d
```

Nothing here waits for the stack to come up or starts it for you; if no broker
answers at `localhost:9092,9094,9095`, the whole suite skips itself with a clear
message rather than failing.

Run only these:

```
pytest -m integration
```

Skip them:

```
pytest -m "not integration"
```

A bare `pytest` runs both the unit and integration suites.

Every topic these tests touch is a scratch topic named `it-...-<random>`, created and
torn down per test — they never read or write the compose stack's real
`order-lifecycle`/`order-lifecycle.retry`/`order-lifecycle.dlq` topics, so running
this suite alongside a live compose stack with real traffic is safe.
