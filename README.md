```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ g c p ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-gcp/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-gcp/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[GOOGLE CLOUD CLIENT FOR STRYKE // STORAGE + PUB/SUB + SECRET MANAGER + BIGQUERY + FIRESTORE + COMPUTE + RUN + FUNCTIONS + LOGGING + MONITORING + IAM]`

> *"GCP from the pipe."*

Google Cloud client for stryke — Cloud Storage, Pub/Sub, Secret Manager,
BigQuery, Firestore, Compute Engine, Cloud Run, Cloud Functions, Cloud
Logging, Cloud Monitoring, and IAM. Opt-in package tier, kept out of the
stryke core binary so the daily-driver install stays slim.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-aws`](https://github.com/MenkeTechnologies/stryke-aws) · [`stryke-k8s`](https://github.com/MenkeTechnologies/stryke-k8s) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Why this is a package, not a builtin](#0x00-why-this-is-a-package-not-a-builtin)
- [\[0x01\] Scope (v0.19.x)](#0x01-scope-v019x)
- [\[0x02\] Install](#0x02-install)
- [\[0x03\] Auth](#0x03-auth)
- [\[0x04\] Quick start](#0x04-quick-start)
- [\[0x05\] API reference](#0x05-api-reference)
- [\[0x06\] FFI layer](#0x06-ffi-layer)
- [\[0x07\] Tests](#0x07-tests)
- [\[0x08\] Dev workflow](#0x08-dev-workflow)
- [\[0x09\] Layout](#0x09-layout)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Why this is a package, not a builtin

Same rationale as the other `stryke-*` cloud packages. GCP integration
needs an auth chain (Application Default Credentials), a TLS HTTP client,
and JSON serialization — fine to bundle once, opt-in.

`stryke-gcp` ships a thin stryke library plus a Rust cdylib
(`libstryke_gcp.{dylib,so}`) that stryke's FFI bridge dlopens in-process
on first `use GCP`. The cdylib talks to GCP's REST APIs directly via
`reqwest` + `google-cloud-auth` — no heavyweight SDK crate involvement,
no version-conflict tax from chrono / arrow / smithy that the proper SDK
crates currently impose.

## [0x01] Scope (v0.19.x)

| Service | Status |
|---|---|
| Cloud Storage (GCS) | shipped — ls / get / put / rm / head / cp / compose / buckets / get+set IAM policy |
| Pub/Sub | shipped — publish / pull / ack / list+create+delete topics+subs |
| Secret Manager | shipped — access / create / add-version / list |
| Auth identity | shipped — ADC + project resolution |
| BigQuery | shipped — query (jobs.query) + streaming insert + list/get datasets + list/get tables |
| Firestore | shipped — get / set / delete / list / query / create (native-mode REST) |
| Compute Engine | shipped — list/get instances / start / stop / reset / list zones |
| Cloud Run | shipped — list/get services / list revisions (Admin API v2) |
| Cloud Functions | shipped — list / describe (API v2) |
| Cloud Logging | shipped — list entries / write entry / list logs (API v2) |
| Cloud Monitoring | shipped — list time series / list metric descriptors (API v3) |
| IAM | shipped — get policy / test permissions / list+get service accounts |

## [0x02] Install

From a release (no rustc on the consumer machine):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-gcp
```

From a local checkout:

```sh
cd ~/projects/stryke-gcp
cargo build --release            # produces target/release/libstryke_gcp.{dylib,so}
s pkg install -g .               # cdylib lands in ~/.stryke/store/gcp@<version>/
```

Or:

```sh
make install
```

The cdylib is dlopened in-process on first `use GCP`. A shared tokio
runtime + `reqwest::Client` + cached ADC credentials are held in
`OnceCell` — no fork-per-call, no re-running of ADC discovery /
metadata-server / WIF / SA-file lookup. Covers GCS, Pub/Sub, Secret
Manager, BigQuery, and Firestore; further services can be added incrementally.

## [0x03] Auth

Uses **Application Default Credentials** — same chain as `gcloud`:

1. `$GOOGLE_APPLICATION_CREDENTIALS` pointing at a service-account JSON.
2. `gcloud auth application-default login` on dev machines.
3. The GCE / Cloud Run / GKE metadata server when running on GCP.

Set the project once and forget about it:

```sh
export GOOGLE_CLOUD_PROJECT=my-project-id
```

Per call, override with `project => "..."`.

## [0x04] Quick start

```stryke
use GCP::Storage
use GCP::PubSub

# Auth + project check.
p to_json GCP::identity()

# GCS — list, get, put, rm.
val @entries = GCP::Storage::ls "gs://my-bucket/prefix/", delimiter => "/"
for val $e (@entries) {
    p "$e->{type}: $e->{key} ($e->{size} bytes)"
}

GCP::Storage::put "gs://my-bucket/hello.txt",
                  data => "hello stryke",
                  content_type => "text/plain"
p GCP::Storage::get "gs://my-bucket/hello.txt"
GCP::Storage::rm "gs://my-bucket/hello.txt"

# Pub/Sub.
GCP::PubSub::publish "my-topic", "event payload",
                     attrs => { source => "stryke" }

val @msgs = GCP::PubSub::pull "my-sub", max => 10, ack => 1
for val $m (@msgs) {
    p "got $m->{message_id}: $m->{data}"
}

# pump = pull → callback → ack each
GCP::PubSub::pump "my-sub",
    iterations => 5,
    callback => fn { handle_message _->{data} }
```

Project / endpoint overrides on every public fn:

```stryke
GCP::Storage::ls "gs://my-bucket/", project => "other-project"
GCP::PubSub::publish "my-topic", "x", project => "other-project"
```

## [0x05] API reference

### `use GCP`

Plumbing: `GCP::version()` (cdylib package version), `GCP::ping(%opts)`
(connectivity probe), `GCP::identity(%opts)` → `{ ok, project,
credentials_source }`, plus the flat `GCP::<service>_<op>` fns the
namespaced wrappers below delegate to.

Pure helpers — credential-free string parsing/validation:

```stryke
GCP::parse_gs_uri($uri)        → { bucket, object }
GCP::build_gs_uri($b, $obj?)   → $uri        # bucket+object → gs:// URI; inverse of parse_gs_uri
GCP::gs_uri_to_url($uri)       → { url, bucket, object }   # gs://b/o → https://storage.googleapis.com/b/o
GCP::url_to_gs_uri($url)       → { uri, bucket, object }   # GCS URL (path/virtual-hosted) → gs://b/o; inverse of gs_uri_to_url
GCP::parse_resource_name($n)   → { parts, pairs:{collection=>id}, trailing }   # projects/p/topics/t
GCP::resource_name_parent($n)  → { name, parent, has_parent, collection, id }  # hierarchy "dirname": projects/p/locations/l/clusters/c → parent projects/p/locations/l; top-level parent is ""
GCP::build_resource_name(%opts) → $name   # { parts } or { pairs, trailing } → resource name; inverse of parse_resource_name
GCP::parse_table_ref($ref) → { table_ref, project, dataset, table, legacy }   # BigQuery project.dataset.table (standard) or project:dataset.table (legacy)
GCP::build_table_ref(%opts) → { table_ref, project, dataset, table, legacy }   # inverse: {dataset,table[,project,legacy]} → reference string (round-trips parse_table_ref)
GCP::valid_bucket_name($name)  → { name, valid, reason }   # GCS rules (underscores OK, no `goog`/`google`)
GCP::valid_project_id($id)     → { id, valid, reason }     # project ID: 6-30 lowercase/digit/hyphen, start letter, no trailing hyphen
GCP::valid_object_name($name)  → { name, valid, reason }   # GCS object name hard rules: 1-1024 UTF-8 bytes, no CR/LF, not ./.., no .well-known/acme-challenge/ prefix
GCP::region_for_zone($zone)    → { zone, region, zone_letter }   # zone → region (us-central1-a → us-central1)
GCP::valid_label($key, $value?) → { key, value, valid, reason }   # Resource Manager label: key 1-63 start-lowercase-letter, value 0-63, lowercase/digit/_/-
GCP::valid_pubsub_id($id)      → { id, valid, reason }   # Pub/Sub topic/subscription/schema/snapshot id: 3-255 chars, start-letter, no goog prefix, letters/digits/-_.~+%
GCP::valid_service_account_id($id) → { id, valid, reason }   # IAM accountId: 6-30 chars, RFC1035 (lowercase/digit/hyphen, start-letter, no trailing hyphen)
GCP::valid_secret_id($id)        → { id, valid, reason }   # Secret Manager secret ID: 1-255 chars, letters/numerals/-/_ (no leading/trailing restrictions)
GCP::valid_dataset_id($id)     → { id, valid, reason }   # BigQuery dataset id: ≤1024 chars, letters/digits/underscores only (case-sensitive)
GCP::valid_table_id($id)       → { id, valid, reason, bytes }   # BigQuery table id: ≤1024 UTF-8 bytes, Unicode letters/numbers + _ - space (broader than dataset)
```

### `use GCP::Storage`

```stryke
GCP::Storage::ls       $uri, %opts → @entries
GCP::Storage::get      $uri, %opts → $body | $path (when output=>"PATH")
GCP::Storage::put      $uri, %opts → \%resp         # data=>$bytes | input=>"PATH"
GCP::Storage::head     $uri, %opts → \%meta         # size, content_type, updated, md5_hash, …
GCP::Storage::cp       $src_uri, $dst_uri, %opts → \%resp   # server-side copy
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
GCP::PubSub::topics       %opts → @topic_names
GCP::PubSub::subs         %opts → @{ {name, topic} }
GCP::PubSub::create_topic $topic, %opts → \%resp
GCP::PubSub::create_sub   $name, $topic, %opts → \%resp   # opts: ack_deadline
GCP::PubSub::delete_topic $topic, %opts → \%resp
GCP::PubSub::delete_sub   $sub, %opts → \%resp
GCP::PubSub::pump         $sub, %opts → $count             # callback + auto-ack
```

### `use GCP::BigQuery`

```stryke
GCP::BigQuery::query $sql, %opts → { columns, rows, total_rows, complete }
                                                # opts: max_results, timeout_ms, project
GCP::BigQuery::rows  $sql, %opts → @rows        # just the row hashrefs
GCP::BigQuery::insert $dataset, $table, \@rows, %opts → { inserted, errors }  # streaming insert
```

### `use GCP::Firestore`

```stryke
GCP::Firestore::get    $collection, $document, %opts → \%data | undef
GCP::Firestore::set    $collection, $document, \%data, %opts → \%resp  # create-or-overwrite
GCP::Firestore::delete $collection, $document, %opts → \%resp
GCP::Firestore::list   $collection, %opts → @{ {id, data} }   # opt: page_size
GCP::Firestore::query  $collection, %opts → @{ {id, data} }   # opts: field, op, value, limit
GCP::Firestore::create $collection, \%data, %opts → { collection, id }  # auto-id; opt: document
```

Field values cross as plain stryke data; the cdylib handles Firestore's typed
encoding (`stringValue`/`integerValue`/…) in both directions. Flat forms are
`GCP::firestore_get` / `_set` / `_delete` / `_list`.

`use GCP::Storage` also gains `GCP::Storage::compose($bucket, $dst, \@sources)`
(concatenate objects) plus `GCP::Storage::get_iam_policy($bucket)` /
`GCP::Storage::set_iam_policy($bucket, \%policy)` — flat forms
`GCP::gcs_compose` / `GCP::gcs_get_iam_policy` / `GCP::gcs_set_iam_policy`.

### `use GCP::Compute`

```stryke
GCP::Compute::instances $zone, %opts → @{ {name, status, machine_type, zone} }
GCP::Compute::get       $zone, $instance, %opts → { zone, instance, resource }
GCP::Compute::start     $zone, $instance, %opts → { instance, action, operation, status }
GCP::Compute::stop      $zone, $instance, %opts → { instance, action, operation, status }
GCP::Compute::reset     $zone, $instance, %opts → { instance, action, operation, status }
GCP::Compute::zones     %opts → @{ {name, region, status} }
```

### `use GCP::Run`

```stryke
GCP::Run::services  $region, %opts → @{ {name, uri, latest_revision} }
GCP::Run::get       $region, $service, %opts → { region, service, resource }
GCP::Run::revisions $region, $service, %opts → @{ {name, create_time} }
```

### `use GCP::Functions`

```stryke
GCP::Functions::list     %opts → @{ {name, state, url} }   # opt region (default "-" = all)
GCP::Functions::describe $region, $function, %opts → { region, function, resource }
```

### `use GCP::Logging`

```stryke
GCP::Logging::entries %opts → @{ {timestamp, severity, log_name, text_payload, json_payload} }
                                              # opts: filter, page_size (50), order (asc/desc)
GCP::Logging::write   $log, %opts → { log, written }   # opts: text=>$str | json=>\%payload, severity
GCP::Logging::logs    %opts → @log_ids
```

### `use GCP::Monitoring`

```stryke
GCP::Monitoring::time_series $filter, $start, $end, %opts → @series   # interval is RFC3339
GCP::Monitoring::descriptors %opts → @{ {type, display_name, kind, value_type} }   # opt filter
```

### `use GCP::IAM`

```stryke
GCP::IAM::get_policy       $url, %opts → \%policy          # full :getIamPolicy endpoint
GCP::IAM::test_permissions $url, \@perms, %opts → @granted # full :testIamPermissions endpoint
GCP::IAM::service_accounts %opts → @{ {email, display_name, unique_id} }
GCP::IAM::service_account  $email, %opts → { email, resource }
```

### `use GCP::BigQuery` — datasets & tables

```stryke
GCP::BigQuery::datasets %opts → @dataset_ids
GCP::BigQuery::dataset  $dataset, %opts → { dataset, resource }
GCP::BigQuery::tables   $dataset, %opts → @table_ids
GCP::BigQuery::table    $dataset, $table, %opts → { dataset, table, resource }
```

### `use GCP` — Secret Manager

```stryke
GCP::secret_access      $secret, %opts → $value    # opts: version (default "latest")
GCP::secret_create      $secret, %opts → \%resp    # new empty secret, automatic replication
GCP::secret_add_version $secret, $value, %opts → \%resp
GCP::secret_list        %opts → @secret_ids
```

Topic / subscription names accept bare (`my-topic`) or fully qualified
(`projects/PROJECT/topics/my-topic`) forms. Bare names expand against
`$opts{project}` or `$GOOGLE_CLOUD_PROJECT`.

## [0x06] FFI layer

Each `GCP::*` wrapper builds a JSON args dict and calls a sibling
`gcp__*` symbol resolved out of `libstryke_gcp.{dylib,so}`. The cdylib
is dlopened in-process on first `use GCP` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook) and exposes the entry
points listed in the `[ffi]` exports table in `stryke.toml`, spanning
identity, GCS, Pub/Sub, Secret Manager, BigQuery, Firestore, Compute
Engine, Cloud Run, Cloud Functions, Cloud Logging, Cloud Monitoring,
and IAM.

**Persistent state:** a shared tokio runtime + `reqwest::Client` +
cached ADC credentials held in `OnceCell` — no fork-per-call, no
re-running of ADC discovery / metadata-server / WIF / SA-file lookup
on each call.

Errors come back as `{"error": "<msg>"}` — the wrapper `die`s with it.

## [0x07] Tests

```sh
cargo test                                          # compiles, no live calls
s test t/                                           # ADC-aware end-to-end

# Opt into per-service round-trips:
export STRYKE_GCP_TEST_BUCKET=my-test-bucket
export STRYKE_GCP_TEST_TOPIC=my-test-topic
export STRYKE_GCP_TEST_SUB=my-test-sub
s test t/
```

The suite skips cleanly when the cdylib isn't installed, when ADC isn't
reachable, or when the per-service env vars are unset.

## [0x08] Dev workflow

```sh
make             # release build
make debug
make test
make install
make clean
```

## [0x09] Layout

```
stryke-gcp/
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # cdylib crate manifest
  Makefile
  src/
    lib.rs                         # cdylib — gcp__* extern "C" exports
  lib/
    GCP.stk                        # `use GCP` — plumbing + ping + identity + flat ops
    Storage.stk                    # `use GCP::Storage`
    PubSub.stk                     # `use GCP::PubSub`
    BigQuery.stk                   # `use GCP::BigQuery`
    Firestore.stk                  # `use GCP::Firestore`
    Compute.stk                    # `use GCP::Compute`
    Run.stk                        # `use GCP::Run`
    Functions.stk                  # `use GCP::Functions`
    Logging.stk                    # `use GCP::Logging`
    Monitoring.stk                 # `use GCP::Monitoring`
    IAM.stk                        # `use GCP::IAM`
  t/
    test_gcp.stk                   # end-to-end (gated on ADC + opt-in env vars)
    test_stryke_gcp_surface.stk    # wrapper-completeness pin
  examples/
    discover.stk
    gcs_browse.stk
    gcs_put_get.stk
    pubsub_pump.stk
    whoami.stk
  .github/workflows/
    ci.yml                         # cargo check/test/clippy + docs lint
    release.yml                    # cross-compile + GH release on tag push
```

## [0xFF] License

MIT.
