# stryke-gcp

Google Cloud client for stryke — Cloud Storage and Pub/Sub. Opt-in package
tier, kept out of the stryke core binary so the daily-driver install stays
slim.

Created by MenkeTechnologies.

## Why this is a package, not a builtin

Same rationale as the other `stryke-*` cloud packages. GCP integration
needs an auth chain (Application Default Credentials), a TLS HTTP client,
and JSON serialization — fine to bundle once, opt-in.

`stryke-gcp` ships a thin stryke library plus a Rust helper binary
(`stryke-gcp-helper`, ~5.5 MB). The helper talks to GCP's REST APIs
directly via `reqwest` + `google-cloud-auth` — no heavyweight SDK crate
involvement, no version-conflict tax from chrono / arrow / smithy that the
proper SDK crates currently impose.

## Scope (v1)

| Service | Status |
|---|---|
| Cloud Storage (GCS) | shipped — ls / get / put / head / rm / buckets |
| Pub/Sub | shipped — publish / pull / ack / topics / subs |
| Auth identity | shipped — ADC + project resolution |
| BigQuery | **deferred v2** — official Rust crate's dep tree (arrow-arith + chrono) has unresolved trait-method ambiguity; will revisit with a REST-only path. |
| Firestore | **deferred v2** |
| Cloud Functions / Run | **deferred v2** |

## Install

```sh
cd ~/projects/stryke-gcp
cargo build --release            # produces target/release/stryke-gcp-helper
s pkg install -g .               # installs `gcp` and `gcp-build` CLIs
```

Or:

```sh
make install
```

## Auth

Uses **Application Default Credentials** — same chain as `gcloud`:

1. `$GOOGLE_APPLICATION_CREDENTIALS` pointing at a service-account JSON.
2. `gcloud auth application-default login` on dev machines.
3. The GCE / Cloud Run / GKE metadata server when running on GCP.

Set the project once and forget about it:

```sh
export GOOGLE_CLOUD_PROJECT=my-project-id
```

Per call, override with `--project=...` or `project => "..."`.

## Quick start

```stryke
use GCP::Storage
use GCP::PubSub

# Auth + project check.
p to_json GCP::auth()

# GCS — list, get, put, head, rm.
my @entries = GCP::Storage::ls "gs://my-bucket/prefix/", delimiter => "/"
for my $e (@entries) {
    p "$e->{type}: $e->{key} ($e->{size} bytes)"
}

GCP::Storage::put "gs://my-bucket/hello.txt",
                  data => "hello stryke",
                  content_type => "text/plain"
p GCP::Storage::get "gs://my-bucket/hello.txt"
p to_json GCP::Storage::head "gs://my-bucket/hello.txt"
GCP::Storage::rm "gs://my-bucket/hello.txt"

# Pub/Sub.
GCP::PubSub::publish "my-topic", "event payload",
                     attrs => { source => "stryke" }

my @msgs = GCP::PubSub::pull "my-sub", max => 10, ack => 1
for my $m (@msgs) {
    p "got $m->{message_id}: $m->{data}"
}

# pump = pull → callback → ack each
GCP::PubSub::pump "my-sub",
    iterations => 5,
    callback => sub ($m) { handle_message $m->{data} }
```

Project / endpoint overrides on every public fn:

```stryke
GCP::Storage::ls "gs://my-bucket/", project => "other-project"
GCP::PubSub::publish "my-topic", "x", project => "other-project"
```

## CLI: `gcp`

```sh
gcp gcs ls gs://bucket/prefix/ --delimiter=/
gcp gcs get gs://bucket/key --output=local.bin
gcp gcs put gs://bucket/key --input=local.bin --content-type=image/png
gcp gcs head gs://bucket/key
gcp gcs rm gs://bucket/key
gcp gcs buckets

gcp pubsub publish my-topic --data='hello' --attr source=cli
gcp pubsub pull    my-sub --max=10 --ack
gcp pubsub ack     my-sub --ids=ABC,DEF
gcp pubsub topics
gcp pubsub subs

gcp auth                                  # current project + ADC source
gcp ping                                  # alias for auth (exit 0 on success)
gcp build                                 # cargo build --release
gcp version
```

Global flags:

```
-p, --project PROJECT         $GOOGLE_CLOUD_PROJECT
```

The helper has no `--region` or `--endpoint` flags — GCP is global by
project, and the API endpoints are universal.

## API reference

### `use GCP`

Plumbing: `GCP::helper_path()`, `GCP::ensure_built()`, `GCP::version()`,
`GCP::ping(%opts)`, `GCP::auth(%opts)`.

### `use GCP::Storage`

```stryke
GCP::Storage::ls       $uri, %opts → @entries
GCP::Storage::get      $uri, %opts → $body | $path (when output=>"PATH")
GCP::Storage::put      $uri, %opts → \%resp         # data=>$bytes | input=>"PATH"
GCP::Storage::head     $uri, %opts → \%resp
GCP::Storage::rm       $uri, %opts → \%resp
GCP::Storage::buckets  %opts → @buckets
```

`ls` entries: `{type=>"object", key, size, content_type, md5, etag,
updated, storage_class, generation}` or `{type=>"prefix", key}` when
`delimiter` is set.

### `use GCP::PubSub`

```stryke
GCP::PubSub::publish  $topic, $data, %opts → \%resp     # opts: attrs=>{...}, ordering_key
GCP::PubSub::pull     $sub, %opts → @messages          # opts: max, deadline, ack
GCP::PubSub::ack      $sub, $ids_or_aref, %opts → \%resp
GCP::PubSub::topics   %opts → @names
GCP::PubSub::subs     %opts → @sub_objects
GCP::PubSub::pump     $sub, %opts → $count             # callback + auto-ack
```

Topic / subscription names accept bare (`my-topic`) or fully qualified
(`projects/PROJECT/topics/my-topic`) forms. Bare names expand against
`$opts{project}` or `$GOOGLE_CLOUD_PROJECT`.

## Helper protocol

```sh
stryke-gcp-helper gcs ls gs://bucket/prefix --delimiter=/
stryke-gcp-helper gcs put gs://bucket/k --input=- < file
stryke-gcp-helper pubsub publish my-topic --data='hello' --attr k=v
stryke-gcp-helper pubsub pull my-sub --max=10 --ack
stryke-gcp-helper auth
```

Output:

* List / stream commands → NDJSON, one JSON object per line.
* Single-object commands → one JSON object + newline.
* All errors → exit non-zero, message on stderr.

## Tests

```sh
cargo test                                          # compiles, no live calls
s test t/                                           # ADC-aware end-to-end

# Opt into per-service round-trips:
export STRYKE_GCP_TEST_BUCKET=my-test-bucket
export STRYKE_GCP_TEST_TOPIC=my-test-topic
export STRYKE_GCP_TEST_SUB=my-test-sub
s test t/
```

The suite skips cleanly when the helper isn't built, when ADC isn't
reachable, or when the per-service env vars are unset.

## Dev workflow

```sh
make             # release build
make debug
make test
make install
make clean
```

## Layout

```
stryke-gcp/
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # Rust helper crate manifest
  Makefile
  src/
    main.rs                        # CLI dispatch
    common.rs                      # ADC + REST plumbing
    gcs.rs                         # GCS JSON API
    pubsub.rs                      # Pub/Sub REST API
    auth.rs                        # identity check
  lib/
    GCP.stk                        # `use GCP` — plumbing + ping + auth
    Storage.stk                    # `use GCP::Storage`
    PubSub.stk                     # `use GCP::PubSub`
  bin/
    gcp.stk                        # `gcp` CLI
    gcp-build.stk
  t/
    test_gcp.stk                   # end-to-end (gated on ADC + opt-in env vars)
  examples/
    gcs_browse.stk
    gcs_put_get.stk
    pubsub_pump.stk
  .github/workflows/
    ci.yml                         # cargo + fake-gcs / pubsub emulator
    release.yml                    # cross-compile + GH release on tag push
```

## License

MIT.
