<div align="center">

# Stutter

**Delivery-fault testing for message-bus consumers.**

A side project built around one question: can you find non-idempotent message
handlers *automatically* — by replaying real traffic into a service while
committing the delivery faults its bus is actually permitted to commit, and
watching what changes?

[![Go](https://img.shields.io/badge/Go-1.27-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![NATS](https://img.shields.io/badge/NATS-JetStream%202.14-27AAE1?logo=natsdotio&logoColor=white)](https://nats.io)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)
[![Status](https://img.shields.io/badge/status-personal%20project%20%C2%B7%20pre--alpha-orange)](#status)

</div>

---

## Why

The ticket always reads the same: **"customer charged twice, can't reproduce,
closing as a one-off."**

It isn't one. NATS JetStream, RabbitMQ under manual acknowledgement and SQS
standard queues all default to *at-least-once* delivery, as does Kafka's
producer. That is not a promise you will see a duplicate — it is the absence of
a promise that you won't. AWS says only that more than one copy of a message
*might* be delivered. Stronger modes exist, and most services are not running in
them.

Nothing crashes when a duplicate does arrive, because the output is data, so it
surfaces weeks later as a reconciliation that won't balance. Tests deliver each
message once, in order: every handler is correct in isolation, and the bug lives
in the interleaving.

This catches careful teams. One open-source bulk mail service traced a run of
duplicate customer emails to a backoff curve that gave every consumer a
one-second first acknowledgement deadline — shorter than an SMTP handover, so the
server redelivered mid-send and roughly one delivery in six went out twice. No
handler was wrong. The deadline lived in a different file.

`SET stock = 12` survives a redelivery. `SET stock = stock - 3` does not. The week
you find one, the question stops being *what happened here* and becomes **where
else is this true?**

The usual answer is to hand-write a "deliver it twice" test for each handler, and
keep remembering to. I wanted to know whether that could be done for you instead
— whether something could read a consumer's own configuration, work out which
faults its bus is genuinely allowed to commit, commit them, and report which
handler changed behaviour. Stutter is me finding out.

Plenty of the pieces exist elsewhere. Protocol-aware proxies inject duplicate
messages; record-and-replay tools capture real traffic and diff what a service
did with it. What kept my interest was the join: choosing faults from the
recorded delivery contract rather than from a percentage knob, deciding
automatically whether a fault broke anything, and shrinking a failure to the
smallest message sequence that still reproduces it.

## Status

**Pre-alpha, and a personal project.** It runs end to end against its own
reference consumer and finds the bug planted there. It cannot yet be pointed at
your service.

Built to explore the idea rather than to be adopted: clone it, take it apart,
lift whatever is useful. There is no roadmap commitment, no support, and it is
shaped around exactly one stack — NATS JetStream and Postgres — so your mileage
outside that will be poor.

**Implemented:**
- Corpus stored as a real JetStream stream, so replay uses real consumers and
  real acknowledgements — a duplicate is an actual server redelivery, not a model
- Faults: `duplicate`, `crash-before-ack`, `delay`, `reorder`, each injected only
  where the recorded consumer configuration permits it
- A legality table read off the consumer's own config, including the backoff
  curve that overrides the declared ack wait
- Effect observation for **both** dependencies — the database *and* the bus, so a
  key/value idempotency claim is visible rather than invisible
- A determinism gate: no findings are reported until two unmutated runs agree
  byte for byte
- Delta-debugging shrink to a minimal reproducing message set
- A report that names the consumer, the fault, the configuration clause that made
  the fault legal, and the minimal repro — plus exit codes for CI

**Rough edges / not done:**
- **Only the built-in reference consumer.** Pointing it at your own service needs
  compose provisioning, which is not built
- Postgres only for datastore effects — there is no proxy for any other engine,
  so a service using one is not observed at all rather than partially
- No HTTP effect source yet, so a handler whose guard is an API call is judged on
  partial evidence
- The differential re-keying gate is designed but not built; it arrives with the
  redaction pipeline
- `concurrent` delivery is refused rather than injected — it needs per-connection
  effect attribution, and injecting it today would mis-attribute effects and
  manufacture false positives
- No recorder against a live bus, and therefore no redaction pipeline: everything
  runs on a synthetic corpus

**Explicitly declined (not on the roadmap):**
- Any hosted or SaaS component • telemetry • multi-bus support before NATS is
  genuinely done • load or performance testing — this is a correctness tool

## What a run looks like

```
$ stutter check --postgres "$DSN" --max-runs 3

Scanned 1 consumer over 3 recorded messages.
Gates held: determinism.

FAIL  reserve_stock          duplicate delivery       the service did different work under the fault
      minimal repro: messages #1
      mutated run: pg.query INSERT INTO audit (id, order_id, at) VALUES ($1, $2, $3)
      legal because: AckPolicy: explicit with MaxDeliver 6 — an unacknowledged message is redelivered

WARN  reserve_stock          duplicate delivery       the service did different work under the fault
      minimal repro: messages #3
      mutated run: nats.publish kv.create bucket=claims key=3
      a repeated outbound call may be a charge or may be harmless and Stutter cannot tell
      which — declare an invariant to promote or silence it
```

Exit codes: `0` all clear · `1` at least one failure · `2` a gate was violated so
nothing was computed, which is **not** a test failure · `3` setup error.

## How it works

A fault is not simulated. The corpus is a real JetStream stream, so withholding an
acknowledgement makes the *server* redeliver — the same thing that happens when a
worker dies between its side effect and its ack:

```mermaid
sequenceDiagram
    autonumber
    participant Bus as JetStream
    participant H as Handler
    participant DB as Postgres

    rect rgba(128,128,128,0.12)
    Note over Bus,DB: reference run
    Bus->>H: deliver order.created seq 8891
    H->>DB: UPDATE stock SET qty = qty - 3
    H->>Bus: ack
    end

    rect rgba(128,128,128,0.12)
    Note over Bus,DB: same corpus, ack withheld once
    Bus->>H: deliver order.created seq 8891
    H->>DB: UPDATE stock SET qty = qty - 3
    Note over H,Bus: ack withheld — this is the whole fault
    Bus->>H: redeliver order.created seq 8891
    H->>DB: UPDATE stock SET qty = qty - 3
    H->>Bus: ack
    Note over DB: stock decremented twice
    end
```

Which faults are even attempted comes from the recorded consumer's own
configuration, not from a knob. `MaxAckPending: 1` forbids reordering.
`MaxDeliver: 1` forbids redelivery entirely. A backoff curve, where one is set,
overrides the declared ack wait and decides how long a `delay` must hold.

To see what the handler did, both of its egress paths are routed through proxies
that record into one ordered sequence. Watching only the database would miss a
guard that claims a key on the bus:

```mermaid
flowchart LR
    C[("Corpus<br/>embedded JetStream")]
    SUT["Service under test<br/>(unmodified)"]
    PGP["Postgres proxy"]
    BUSP["Bus proxy"]
    PG[("Ephemeral Postgres<br/>wiped between runs")]
    REC["Recorder<br/>canonicalise · attribute"]
    GATE["Gate · compare · shrink"]

    C -->|"one message in flight"| SUT
    SUT -->|SQL| PGP
    SUT -->|"publish · KV claim"| BUSP
    PGP --> PG
    BUSP --> C
    PGP -.->|effect| REC
    BUSP -.->|effect| REC
    REC --> GATE
```

Nothing is reported until two unmutated runs produce a byte-identical sequence.
A violated gate exits `2` with no findings at all, rather than a list of findings
carrying a caveat.

## Design

**Only do what the bus can actually do.** A fuzzer that invents impossible
interleavings produces unfixable reports and destroys trust in its own output.
Every fault is derived from the recorded delivery contract, and every finding
carries the configuration clause that licensed it — so it is always answerable
with *yes, the bus really can do that to you*.

**Your service is not modified.** It runs with its egress routed through proxies
Stutter controls, so effects are observed without an agent, an SDK, or a code
change.

**Nothing is reported until the reference is reproducible.** Two unmutated runs
must produce a byte-identical effect sequence first. A violated gate withholds
the report rather than qualifying it, because a caveated finding list is worse
than none.

**Bias to under-report.** One false positive voids trust in every other line.

## Requirements

Go 1.27+ and a scratch Postgres to replay into. The bus is embedded, so there is
nothing to install for it; Docker is simply the quickest way to get the database.

## Development

```sh
docker run -d --name stutter-pg -e POSTGRES_PASSWORD=stutter \
  -e POSTGRES_USER=stutter -e POSTGRES_DB=stutter -p 55432:5432 postgres:18-alpine
export STUTTER_TEST_POSTGRES='postgres://stutter:stutter@127.0.0.1:55432/stutter?sslmode=disable'

make ci      # format check, lint, tidy, race tests, vulnerability scan, build
```

The replay suite **skips** without `STUTTER_TEST_POSTGRES` and prints the same
`ok` as a real pass, so set it or you are testing far less than you think.

Linting is `golangci-lint` with every linter enabled by default; each exception
is justified inline in `.golangci.yml`.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
