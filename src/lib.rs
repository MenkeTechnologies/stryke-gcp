//! stryke-gcp — Google Cloud cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn gcp__*` is a JSON-string-in /
//! JSON-string-out wrapper around the GCS + Pub/Sub REST APIs (via
//! `reqwest`) authenticated with `google-cloud-auth` (ADC chain).
//! stryke's FFI bridge (`rust_ffi.rs::load_cdylib`) resolves these
//! symbols at first `use GCP`.
//!
//! Persistent state:
//!   * `RUNTIME` — one shared `tokio` runtime drives every async call.
//!   * `CLIENT` — one shared `reqwest::Client` with rustls-tls.
//!   * `CREDS` — `google_cloud_auth::credentials::Credentials` cache
//!     for the lifetime of the process. v1 helper re-ran ADC discovery
//!     per fork — paying full GCE metadata-server / WIF / SA-file
//!     lookup each call.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;

use anyhow::{anyhow, Context, Result};
use once_cell::sync::OnceCell;
use reqwest::Client;
use serde_json::{json, Value};
use tokio::runtime::{Builder, Runtime};

// ── runtime + client + auth ─────────────────────────────────────────────────

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn rt() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

static CLIENT: OnceCell<Client> = OnceCell::new();

fn client() -> &'static Client {
    CLIENT.get_or_init(|| {
        Client::builder()
            .use_rustls_tls()
            .build()
            .expect("reqwest client")
    })
}

static CREDS: OnceCell<google_cloud_auth::credentials::Credentials> = OnceCell::new();

async fn creds() -> Result<&'static google_cloud_auth::credentials::Credentials> {
    if let Some(c) = CREDS.get() {
        return Ok(c);
    }
    let c = google_cloud_auth::credentials::Builder::default()
        .build()
        .context("loading default GCP credentials")?;
    let _ = CREDS.set(c);
    Ok(CREDS.get().expect("creds set"))
}

async fn auth_header() -> Result<String> {
    use google_cloud_auth::credentials::CacheableResource;
    let c = creds().await?;
    let resource = c
        .headers(http::Extensions::new())
        .await
        .context("fetching access token")?;
    let headers = match resource {
        CacheableResource::New { data, .. } => data,
        CacheableResource::NotModified => {
            return Err(anyhow!(
                "credentials returned NotModified without prior cached headers"
            ));
        }
    };
    let auth = headers
        .get("authorization")
        .ok_or_else(|| anyhow!("no Authorization header from credentials"))?;
    Ok(auth.to_str()?.to_string())
}

fn resolve_project(opts: &Value) -> Option<String> {
    opts.get("project")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| std::env::var("GOOGLE_CLOUD_PROJECT").ok())
        .or_else(|| std::env::var("GCLOUD_PROJECT").ok())
}

// ── identity ────────────────────────────────────────────────────────────────

async fn op_identity(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts);
    // Touching auth_header confirms ADC reachability.
    let _ = auth_header().await?;
    let creds_path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok();
    let source = if creds_path.is_some() {
        "GOOGLE_APPLICATION_CREDENTIALS file"
    } else {
        "Application Default Credentials chain"
    };
    Ok(json!({
        "ok": true,
        "project": project,
        "credentials_source": source,
        "credentials_path": creds_path,
    }))
}

// ── GCS ─────────────────────────────────────────────────────────────────────

async fn op_gcs_list_buckets(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts)
        .ok_or_else(|| anyhow!("missing project (set GOOGLE_CLOUD_PROJECT or pass `project`)"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b?project={}",
        urlencoding::encode(&project)
    );
    let resp = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?;
    let body: Value = resp.json().await?;
    let names: Vec<String> = body["items"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|b| b["name"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"project": project, "buckets": names}))
}

async fn op_gcs_list_objects(opts: Value) -> Result<Value> {
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?;
    let prefix = opts["prefix"].as_str().unwrap_or("");
    let token = auth_header().await?;
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o?prefix={}",
        urlencoding::encode(bucket),
        urlencoding::encode(prefix)
    );
    let resp = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?;
    let body: Value = resp.json().await?;
    let items: Vec<Value> = body["items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|o| {
            json!({
                "name": o["name"].as_str().unwrap_or(""),
                "size": o["size"].as_str().unwrap_or("0"),
                "content_type": o["contentType"].as_str().unwrap_or(""),
            })
        })
        .collect();
    Ok(json!({"bucket": bucket, "objects": items}))
}

async fn op_gcs_get_object(opts: Value) -> Result<Value> {
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?;
    let key = opts["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}?alt=media",
        urlencoding::encode(bucket),
        urlencoding::encode(key)
    );
    let resp = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?;
    let bytes = resp.bytes().await?;
    let body = match std::str::from_utf8(&bytes) {
        Ok(s) => Value::String(s.to_string()),
        Err(_) => {
            use base64::Engine as _;
            Value::String(format!(
                "base64:{}",
                base64::engine::general_purpose::STANDARD.encode(&bytes)
            ))
        }
    };
    Ok(json!({"bucket": bucket, "key": key, "body": body}))
}

async fn op_gcs_put_object(opts: Value) -> Result<Value> {
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?
        .to_string();
    let key = opts["key"]
        .as_str()
        .ok_or_else(|| anyhow!("missing key"))?
        .to_string();
    let body = opts["body"].as_str().unwrap_or("").as_bytes().to_vec();
    let content_type = opts["content_type"]
        .as_str()
        .unwrap_or("application/octet-stream")
        .to_string();
    let token = auth_header().await?;
    let url = format!(
        "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
        urlencoding::encode(&bucket),
        urlencoding::encode(&key)
    );
    let resp = client()
        .post(&url)
        .header("Authorization", token)
        .header("Content-Type", content_type)
        .body(body)
        .send()
        .await?
        .error_for_status()?;
    let info: Value = resp.json().await?;
    Ok(json!({
        "bucket": bucket,
        "key": key,
        "generation": info["generation"].clone(),
        "etag": info["etag"].clone(),
    }))
}

async fn op_gcs_delete_object(opts: Value) -> Result<Value> {
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?
        .to_string();
    let key = opts["key"]
        .as_str()
        .ok_or_else(|| anyhow!("missing key"))?
        .to_string();
    let token = auth_header().await?;
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
        urlencoding::encode(&bucket),
        urlencoding::encode(&key)
    );
    client()
        .delete(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?;
    Ok(json!({"bucket": bucket, "key": key, "deleted": true}))
}

// ── Pub/Sub ─────────────────────────────────────────────────────────────────

async fn op_pubsub_publish(opts: Value) -> Result<Value> {
    use base64::Engine as _;
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let topic = opts["topic"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic"))?
        .to_string();
    let data = opts["data"].as_str().unwrap_or("").to_string();
    let attrs = opts["attributes"].as_object().cloned().unwrap_or_default();
    let token = auth_header().await?;
    let url = format!(
        "https://pubsub.googleapis.com/v1/projects/{}/topics/{}:publish",
        urlencoding::encode(&project),
        urlencoding::encode(&topic)
    );
    let body = json!({
        "messages": [{
            "data": base64::engine::general_purpose::STANDARD.encode(&data),
            "attributes": attrs,
        }]
    });
    let resp = client()
        .post(&url)
        .header("Authorization", token)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let info: Value = resp.json().await?;
    let ids: Vec<String> = info["messageIds"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"topic": topic, "message_ids": ids}))
}

async fn op_pubsub_pull(opts: Value) -> Result<Value> {
    use base64::Engine as _;
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let subscription = opts["subscription"]
        .as_str()
        .ok_or_else(|| anyhow!("missing subscription"))?
        .to_string();
    let max = opts["max"].as_i64().unwrap_or(1);
    let token = auth_header().await?;
    let url = format!(
        "https://pubsub.googleapis.com/v1/projects/{}/subscriptions/{}:pull",
        urlencoding::encode(&project),
        urlencoding::encode(&subscription)
    );
    let body = json!({
        "maxMessages": max,
        "returnImmediately": false,
    });
    let resp = client()
        .post(&url)
        .header("Authorization", token)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let info: Value = resp.json().await?;
    let messages: Vec<Value> = info["receivedMessages"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|m| {
            let data_b64 = m["message"]["data"].as_str().unwrap_or("");
            let data = base64::engine::general_purpose::STANDARD
                .decode(data_b64)
                .ok()
                .and_then(|b| String::from_utf8(b).ok())
                .unwrap_or_default();
            json!({
                "ack_id": m["ackId"].as_str().unwrap_or(""),
                "message_id": m["message"]["messageId"].as_str().unwrap_or(""),
                "data": data,
                "attributes": m["message"]["attributes"].clone(),
            })
        })
        .collect();
    Ok(json!({"subscription": subscription, "messages": messages}))
}

async fn op_pubsub_ack(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let subscription = opts["subscription"]
        .as_str()
        .ok_or_else(|| anyhow!("missing subscription"))?
        .to_string();
    let ack_ids: Vec<String> = opts["ack_ids"]
        .as_array()
        .ok_or_else(|| anyhow!("missing ack_ids (array)"))?
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    let token = auth_header().await?;
    let url = format!(
        "https://pubsub.googleapis.com/v1/projects/{}/subscriptions/{}:acknowledge",
        urlencoding::encode(&project),
        urlencoding::encode(&subscription)
    );
    let body = json!({ "ackIds": ack_ids });
    client()
        .post(&url)
        .header("Authorization", token)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    Ok(json!({"subscription": subscription, "acked": ack_ids.len()}))
}

// ── GCS extras: head + copy ──────────────────────────────────────────────────

/// Object metadata (`HEAD`-equivalent: GET the JSON metadata, no media).
async fn op_gcs_head_object(opts: Value) -> Result<Value> {
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?;
    let key = opts["key"].as_str().ok_or_else(|| anyhow!("missing key"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
        urlencoding::encode(bucket),
        urlencoding::encode(key)
    );
    let resp = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?;
    let m: Value = resp.json().await?;
    Ok(json!({
        "bucket": bucket,
        "key": key,
        "size": m["size"].as_str().unwrap_or("0"),
        "content_type": m["contentType"].as_str().unwrap_or(""),
        "updated": m["updated"].clone(),
        "generation": m["generation"].clone(),
        "md5_hash": m["md5Hash"].clone(),
        "etag": m["etag"].clone(),
    }))
}

/// Server-side copy of an object to another bucket/key.
async fn op_gcs_copy_object(opts: Value) -> Result<Value> {
    let src_bucket = opts["src_bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing src_bucket"))?;
    let src_key = opts["src_key"]
        .as_str()
        .ok_or_else(|| anyhow!("missing src_key"))?;
    let dst_bucket = opts["dst_bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing dst_bucket"))?;
    let dst_key = opts["dst_key"]
        .as_str()
        .ok_or_else(|| anyhow!("missing dst_key"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}/copyTo/b/{}/o/{}",
        urlencoding::encode(src_bucket),
        urlencoding::encode(src_key),
        urlencoding::encode(dst_bucket),
        urlencoding::encode(dst_key)
    );
    let resp = client()
        .post(&url)
        .header("Authorization", token)
        .header("Content-Length", "0")
        .send()
        .await?
        .error_for_status()?;
    let info: Value = resp.json().await?;
    Ok(json!({
        "dst_bucket": dst_bucket,
        "dst_key": dst_key,
        "generation": info["resource"]["generation"].clone(),
    }))
}

// ── Pub/Sub extras: list + create ────────────────────────────────────────────

/// Strip a `projects/<p>/topics/<t>` (or subscriptions) path to its leaf name.
fn leaf_name(full: &str) -> &str {
    full.rsplit('/').next().unwrap_or(full)
}

/// Interpret a JSON value as a boolean flag tolerant of the FFI bridge, which
/// can deliver `key => 1` as a JSON number (not a bool) and string flags.
/// Mirrors the inline coercion `op_build_table_ref` uses for `legacy`.
fn truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_i64().is_some_and(|x| x != 0),
        Value::String(s) => s == "1" || s.eq_ignore_ascii_case("true"),
        _ => false,
    }
}

async fn op_pubsub_list_topics(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://pubsub.googleapis.com/v1/projects/{}/topics",
        urlencoding::encode(&project)
    );
    let resp = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?;
    let body: Value = resp.json().await?;
    let names: Vec<String> = body["topics"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|t| t["name"].as_str().map(|n| leaf_name(n).to_string()))
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"project": project, "topics": names}))
}

async fn op_pubsub_list_subscriptions(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://pubsub.googleapis.com/v1/projects/{}/subscriptions",
        urlencoding::encode(&project)
    );
    let resp = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?;
    let body: Value = resp.json().await?;
    let subs: Vec<Value> = body["subscriptions"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|s| {
                    json!({
                        "name": leaf_name(s["name"].as_str().unwrap_or("")),
                        "topic": leaf_name(s["topic"].as_str().unwrap_or("")),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"project": project, "subscriptions": subs}))
}

async fn op_pubsub_create_topic(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let topic = opts["topic"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic"))?
        .to_string();
    let token = auth_header().await?;
    let url = format!(
        "https://pubsub.googleapis.com/v1/projects/{}/topics/{}",
        urlencoding::encode(&project),
        urlencoding::encode(&topic)
    );
    let resp = client()
        .put(&url)
        .header("Authorization", token)
        .json(&json!({}))
        .send()
        .await?
        .error_for_status()?;
    let info: Value = resp.json().await?;
    Ok(json!({"topic": leaf_name(info["name"].as_str().unwrap_or(&topic)), "created": true}))
}

async fn op_pubsub_create_subscription(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let subscription = opts["subscription"]
        .as_str()
        .ok_or_else(|| anyhow!("missing subscription"))?
        .to_string();
    let topic = opts["topic"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic (the topic to subscribe to)"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://pubsub.googleapis.com/v1/projects/{}/subscriptions/{}",
        urlencoding::encode(&project),
        urlencoding::encode(&subscription)
    );
    let body = json!({
        "topic": format!("projects/{project}/topics/{topic}"),
        "ackDeadlineSeconds": opts["ack_deadline"].as_u64().unwrap_or(10),
    });
    let resp = client()
        .put(&url)
        .header("Authorization", token)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let info: Value = resp.json().await?;
    Ok(
        json!({"subscription": leaf_name(info["name"].as_str().unwrap_or(&subscription)), "created": true}),
    )
}

/// Get one Pub/Sub topic (`topics.get`). opts: topic (required). Returns
/// `{topic, resource}` with the full topic resource.
async fn op_pubsub_get_topic(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let topic = opts["topic"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://pubsub.googleapis.com/v1/projects/{}/topics/{}",
        urlencoding::encode(&project),
        urlencoding::encode(topic)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(json!({"topic": topic, "resource": body}))
}

/// List the subscriptions attached to a topic (`topics.subscriptions.list`).
/// opts: topic (required). Returns `{topic, subscriptions: [<sub id>]}`.
async fn op_pubsub_topic_subscriptions(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let topic = opts["topic"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://pubsub.googleapis.com/v1/projects/{}/topics/{}/subscriptions",
        urlencoding::encode(&project),
        urlencoding::encode(topic)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let subs: Vec<String> = body["subscriptions"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(|n| leaf_name(n).to_string()))
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"topic": topic, "subscriptions": subs}))
}

// ── Secret Manager ───────────────────────────────────────────────────────────

/// Access a secret version's payload. opts: secret, version (default "latest").
/// Returns the decoded payload string.
async fn op_secret_access(opts: Value) -> Result<Value> {
    use base64::Engine as _;
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let secret = opts["secret"]
        .as_str()
        .ok_or_else(|| anyhow!("missing secret"))?
        .to_string();
    let version = opts["version"].as_str().unwrap_or("latest");
    let token = auth_header().await?;
    let url = format!(
        "https://secretmanager.googleapis.com/v1/projects/{}/secrets/{}/versions/{}:access",
        urlencoding::encode(&project),
        urlencoding::encode(&secret),
        urlencoding::encode(version)
    );
    let resp = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?;
    let body: Value = resp.json().await?;
    let data_b64 = body["payload"]["data"].as_str().unwrap_or("");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(data_b64)
        .map_err(|e| anyhow!("decoding secret payload: {e}"))?;
    let value = match String::from_utf8(decoded.clone()) {
        Ok(s) => Value::String(s),
        Err(_) => Value::String(format!(
            "base64:{}",
            base64::engine::general_purpose::STANDARD.encode(&decoded)
        )),
    };
    Ok(json!({"secret": secret, "version": version, "value": value}))
}

/// Create a new secret (no version yet) with automatic replication.
async fn op_secret_create(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let secret = opts["secret"]
        .as_str()
        .ok_or_else(|| anyhow!("missing secret"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://secretmanager.googleapis.com/v1/projects/{}/secrets?secretId={}",
        urlencoding::encode(&project),
        urlencoding::encode(secret)
    );
    let resp = client()
        .post(&url)
        .header("Authorization", token)
        .json(&json!({"replication": {"automatic": {}}}))
        .send()
        .await?
        .error_for_status()?;
    let info: Value = resp.json().await?;
    Ok(json!({"secret": leaf_name(info["name"].as_str().unwrap_or(secret)), "created": true}))
}

/// Add a new version holding `value` to an existing secret. Returns the version name.
async fn op_secret_add_version(opts: Value) -> Result<Value> {
    use base64::Engine as _;
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let secret = opts["secret"]
        .as_str()
        .ok_or_else(|| anyhow!("missing secret"))?;
    let value = opts["value"]
        .as_str()
        .ok_or_else(|| anyhow!("missing value"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://secretmanager.googleapis.com/v1/projects/{}/secrets/{}:addVersion",
        urlencoding::encode(&project),
        urlencoding::encode(secret)
    );
    let data = base64::engine::general_purpose::STANDARD.encode(value.as_bytes());
    let resp = client()
        .post(&url)
        .header("Authorization", token)
        .json(&json!({"payload": {"data": data}}))
        .send()
        .await?
        .error_for_status()?;
    let info: Value = resp.json().await?;
    Ok(json!({"version": leaf_name(info["name"].as_str().unwrap_or("")), "added": true}))
}

/// List the versions of a secret (`secrets.versions.list`, newest first). opts:
/// secret (required). Returns `{secret, versions: [{name, state, create_time}]}`
/// where `name` is the bare version id (e.g. "3" or "latest").
async fn op_secret_list_versions(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let secret = opts["secret"]
        .as_str()
        .ok_or_else(|| anyhow!("missing secret"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://secretmanager.googleapis.com/v1/projects/{}/secrets/{}/versions",
        urlencoding::encode(&project),
        urlencoding::encode(secret)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let versions: Vec<Value> = body["versions"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|v| {
                    json!({
                        "name": leaf_name(v["name"].as_str().unwrap_or("")),
                        "state": v["state"].as_str().unwrap_or(""),
                        "create_time": v["createTime"].as_str().unwrap_or(""),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"secret": secret, "versions": versions}))
}

/// Delete a secret and all its versions (`secrets.delete`). opts: secret
/// (required). Returns `{secret, deleted: true}`.
async fn op_secret_delete(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let secret = opts["secret"]
        .as_str()
        .ok_or_else(|| anyhow!("missing secret"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://secretmanager.googleapis.com/v1/projects/{}/secrets/{}",
        urlencoding::encode(&project),
        urlencoding::encode(secret)
    );
    client()
        .delete(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?;
    Ok(json!({"secret": secret, "deleted": true}))
}

/// Delete a Pub/Sub topic.
async fn op_pubsub_delete_topic(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let topic = opts["topic"]
        .as_str()
        .ok_or_else(|| anyhow!("missing topic"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://pubsub.googleapis.com/v1/projects/{}/topics/{}",
        urlencoding::encode(&project),
        urlencoding::encode(topic)
    );
    client()
        .delete(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?;
    Ok(json!({"topic": topic, "deleted": true}))
}

/// Delete a Pub/Sub subscription.
async fn op_pubsub_delete_subscription(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let subscription = opts["subscription"]
        .as_str()
        .ok_or_else(|| anyhow!("missing subscription"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://pubsub.googleapis.com/v1/projects/{}/subscriptions/{}",
        urlencoding::encode(&project),
        urlencoding::encode(subscription)
    );
    client()
        .delete(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?;
    Ok(json!({"subscription": subscription, "deleted": true}))
}

// ── BigQuery ──────────────────────────────────────────────────────────────────

/// Run a standard-SQL query (jobs.query) and return the rows as objects keyed
/// by the result schema's column names. opts: query (required), max_results,
/// timeout_ms.
async fn op_bigquery_query(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let query = opts["query"]
        .as_str()
        .ok_or_else(|| anyhow!("missing query"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/queries",
        urlencoding::encode(&project)
    );
    let mut body = json!({"query": query, "useLegacySql": false});
    if let Some(n) = opts["max_results"].as_u64() {
        body["maxResults"] = json!(n);
    }
    if let Some(ms) = opts["timeout_ms"].as_u64() {
        body["timeoutMs"] = json!(ms);
    }
    let resp = client()
        .post(&url)
        .header("Authorization", token)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let r: Value = resp.json().await?;
    // Map each row's positional cells onto the schema's field names.
    let fields: Vec<String> = r["schema"]["fields"]
        .as_array()
        .map(|fs| {
            fs.iter()
                .map(|f| f["name"].as_str().unwrap_or("").to_string())
                .collect()
        })
        .unwrap_or_default();
    let rows: Vec<Value> = r["rows"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    let cells = row["f"].as_array().cloned().unwrap_or_default();
                    let mut obj = serde_json::Map::new();
                    for (i, name) in fields.iter().enumerate() {
                        obj.insert(
                            name.clone(),
                            cells.get(i).map(|c| c["v"].clone()).unwrap_or(Value::Null),
                        );
                    }
                    Value::Object(obj)
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({
        "columns": fields,
        "rows": rows,
        "total_rows": r["totalRows"].clone(),
        "complete": r["jobComplete"].as_bool().unwrap_or(true),
    }))
}

/// Stream-insert rows into a BigQuery table (tabledata.insertAll). opts:
/// dataset, table, rows (array of objects). Returns { inserted, errors }.
async fn op_bigquery_insert(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let dataset = opts["dataset"]
        .as_str()
        .ok_or_else(|| anyhow!("missing dataset"))?;
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?;
    let rows = opts["rows"]
        .as_array()
        .ok_or_else(|| anyhow!("missing rows (an array of objects)"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables/{}/insertAll",
        urlencoding::encode(&project),
        urlencoding::encode(dataset),
        urlencoding::encode(table)
    );
    let body = json!({ "rows": rows.iter().map(|r| json!({"json": r})).collect::<Vec<_>>() });
    let resp = client()
        .post(&url)
        .header("Authorization", token)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let r: Value = resp.json().await?;
    let errors = r.get("insertErrors").cloned().unwrap_or(Value::Null);
    Ok(json!({ "inserted": rows.len(), "errors": errors }))
}

/// Compose (concatenate) GCS source objects into a single destination object.
/// opts: bucket, destination, sources (array of object names).
async fn op_gcs_compose(opts: Value) -> Result<Value> {
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?;
    let destination = opts["destination"]
        .as_str()
        .ok_or_else(|| anyhow!("missing destination"))?;
    let sources = opts["sources"]
        .as_array()
        .ok_or_else(|| anyhow!("missing sources (array of object names)"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/o/{}/compose",
        urlencoding::encode(bucket),
        urlencoding::encode(destination)
    );
    let body = json!({
        "sourceObjects": sources
            .iter()
            .filter_map(|s| s.as_str().map(|n| json!({ "name": n })))
            .collect::<Vec<_>>()
    });
    let resp = client()
        .post(&url)
        .header("Authorization", token)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let info: Value = resp.json().await?;
    Ok(json!({
        "bucket": bucket,
        "object": info["name"].as_str().unwrap_or(destination),
        "size": info["size"].clone(),
    }))
}

// ── Firestore ─────────────────────────────────────────────────────────────────

/// Encode a plain JSON value into Firestore's typed-value wire form
/// (`{"stringValue": ...}`, `{"integerValue": "N"}`, etc.).
fn fs_encode(v: &Value) -> Value {
    match v {
        Value::Null => json!({ "nullValue": null }),
        Value::Bool(b) => json!({ "booleanValue": b }),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                // Firestore integers travel as strings.
                json!({ "integerValue": n.to_string() })
            } else {
                json!({ "doubleValue": n.as_f64().unwrap_or(0.0) })
            }
        }
        Value::String(s) => json!({ "stringValue": s }),
        Value::Array(a) => {
            json!({ "arrayValue": { "values": a.iter().map(fs_encode).collect::<Vec<_>>() } })
        }
        Value::Object(o) => {
            let fields: serde_json::Map<String, Value> =
                o.iter().map(|(k, v)| (k.clone(), fs_encode(v))).collect();
            json!({ "mapValue": { "fields": fields } })
        }
    }
}

/// Decode a Firestore typed value back to plain JSON.
fn fs_decode(v: &Value) -> Value {
    let obj = match v.as_object() {
        Some(o) => o,
        None => return Value::Null,
    };
    if let Some(s) = obj.get("stringValue") {
        return s.clone();
    }
    if let Some(s) = obj.get("integerValue") {
        return s
            .as_str()
            .and_then(|x| x.parse::<i64>().ok())
            .map(|n| json!(n))
            .unwrap_or_else(|| s.clone());
    }
    if let Some(d) = obj.get("doubleValue") {
        return d.clone();
    }
    if let Some(b) = obj.get("booleanValue") {
        return b.clone();
    }
    if obj.contains_key("nullValue") {
        return Value::Null;
    }
    if let Some(ts) = obj.get("timestampValue") {
        return ts.clone();
    }
    if let Some(rv) = obj.get("referenceValue") {
        return rv.clone();
    }
    if let Some(av) = obj.get("arrayValue") {
        let vals = av
            .get("values")
            .and_then(|x| x.as_array())
            .cloned()
            .unwrap_or_default();
        return Value::Array(vals.iter().map(fs_decode).collect());
    }
    if let Some(mv) = obj.get("mapValue") {
        let m: serde_json::Map<String, Value> = mv
            .get("fields")
            .and_then(|x| x.as_object())
            .map(|f| f.iter().map(|(k, v)| (k.clone(), fs_decode(v))).collect())
            .unwrap_or_default();
        return Value::Object(m);
    }
    Value::Null
}

/// Encode a `{field: value, ...}` object into Firestore `fields`.
fn fs_fields_encode(data: &Value) -> Value {
    let m: serde_json::Map<String, Value> = data
        .as_object()
        .map(|o| o.iter().map(|(k, v)| (k.clone(), fs_encode(v))).collect())
        .unwrap_or_default();
    Value::Object(m)
}

/// Decode a Firestore document's `fields` back into a plain object.
fn fs_fields_decode(fields: &Value) -> Value {
    let m: serde_json::Map<String, Value> = fields
        .as_object()
        .map(|o| o.iter().map(|(k, v)| (k.clone(), fs_decode(v))).collect())
        .unwrap_or_default();
    Value::Object(m)
}

fn fs_base(project: &str) -> String {
    format!(
        "https://firestore.googleapis.com/v1/projects/{}/databases/(default)/documents",
        urlencoding::encode(project)
    )
}

async fn op_firestore_get(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let collection = opts["collection"]
        .as_str()
        .ok_or_else(|| anyhow!("missing collection"))?;
    let document = opts["document"]
        .as_str()
        .ok_or_else(|| anyhow!("missing document"))?;
    let token = auth_header().await?;
    let url = format!(
        "{}/{}/{}",
        fs_base(&project),
        urlencoding::encode(collection),
        urlencoding::encode(document)
    );
    let resp = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?;
    if resp.status().as_u16() == 404 {
        return Ok(json!({ "collection": collection, "document": document, "data": Value::Null }));
    }
    let body: Value = resp.error_for_status()?.json().await?;
    Ok(json!({
        "collection": collection,
        "document": document,
        "data": fs_fields_decode(&body["fields"]),
        "update_time": body["updateTime"].clone(),
    }))
}

async fn op_firestore_set(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let collection = opts["collection"]
        .as_str()
        .ok_or_else(|| anyhow!("missing collection"))?;
    let document = opts["document"]
        .as_str()
        .ok_or_else(|| anyhow!("missing document"))?;
    let data = opts
        .get("data")
        .filter(|d| d.is_object())
        .ok_or_else(|| anyhow!("missing data (an object)"))?;
    let token = auth_header().await?;
    // PATCH creates-or-overwrites the document with the supplied fields.
    let url = format!(
        "{}/{}/{}",
        fs_base(&project),
        urlencoding::encode(collection),
        urlencoding::encode(document)
    );
    let body = json!({ "fields": fs_fields_encode(data) });
    let resp = client()
        .patch(&url)
        .header("Authorization", token)
        .json(&body)
        .send()
        .await?
        .error_for_status()?;
    let info: Value = resp.json().await?;
    Ok(json!({
        "collection": collection,
        "document": document,
        "update_time": info["updateTime"].clone(),
    }))
}

async fn op_firestore_delete(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let collection = opts["collection"]
        .as_str()
        .ok_or_else(|| anyhow!("missing collection"))?;
    let document = opts["document"]
        .as_str()
        .ok_or_else(|| anyhow!("missing document"))?;
    let token = auth_header().await?;
    let url = format!(
        "{}/{}/{}",
        fs_base(&project),
        urlencoding::encode(collection),
        urlencoding::encode(document)
    );
    client()
        .delete(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?;
    Ok(json!({ "collection": collection, "document": document, "deleted": true }))
}

async fn op_firestore_list(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let collection = opts["collection"]
        .as_str()
        .ok_or_else(|| anyhow!("missing collection"))?;
    let page_size = opts["page_size"].as_u64().unwrap_or(100);
    let token = auth_header().await?;
    let url = format!(
        "{}/{}?pageSize={}",
        fs_base(&project),
        urlencoding::encode(collection),
        page_size
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let docs: Vec<Value> = body["documents"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|d| {
                    // The document id is the last path segment of `name`.
                    let id = d["name"]
                        .as_str()
                        .and_then(|n| n.rsplit('/').next())
                        .unwrap_or("");
                    json!({ "id": id, "data": fs_fields_decode(&d["fields"]) })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({ "collection": collection, "documents": docs }))
}

/// Normalize a caller operator to a Firestore `fieldFilter` op name.
fn fs_op(op: &str) -> &'static str {
    match op {
        "==" | "EQUAL" | "eq" => "EQUAL",
        "!=" | "NOT_EQUAL" | "ne" => "NOT_EQUAL",
        "<" | "LESS_THAN" | "lt" => "LESS_THAN",
        "<=" | "LESS_THAN_OR_EQUAL" | "le" => "LESS_THAN_OR_EQUAL",
        ">" | "GREATER_THAN" | "gt" => "GREATER_THAN",
        ">=" | "GREATER_THAN_OR_EQUAL" | "ge" => "GREATER_THAN_OR_EQUAL",
        "in" | "IN" => "IN",
        "array_contains" | "ARRAY_CONTAINS" => "ARRAY_CONTAINS",
        _ => "EQUAL",
    }
}

async fn op_firestore_query(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let collection = opts["collection"]
        .as_str()
        .ok_or_else(|| anyhow!("missing collection"))?;
    let token = auth_header().await?;
    let mut sq = json!({ "from": [{ "collectionId": collection }] });
    // Optional single field filter: { field, op, value }.
    if let Some(field) = opts["field"].as_str() {
        let op = fs_op(opts["op"].as_str().unwrap_or("EQUAL"));
        sq["where"] = json!({
            "fieldFilter": {
                "field": { "fieldPath": field },
                "op": op,
                "value": fs_encode(opts.get("value").unwrap_or(&Value::Null)),
            }
        });
    }
    if let Some(n) = opts["limit"].as_i64() {
        sq["limit"] = json!(n);
    }
    let url = format!("{}:runQuery", fs_base(&project));
    let body: Value = client()
        .post(&url)
        .header("Authorization", token)
        .json(&json!({ "structuredQuery": sq }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    // runQuery returns a stream array; keep the entries that carry a document.
    let docs: Vec<Value> = body
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|e| {
                    let d = e.get("document")?;
                    let id = d["name"]
                        .as_str()
                        .and_then(|n| n.rsplit('/').next())
                        .unwrap_or("");
                    Some(json!({ "id": id, "data": fs_fields_decode(&d["fields"]) }))
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({ "collection": collection, "documents": docs }))
}

async fn op_firestore_create(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let collection = opts["collection"]
        .as_str()
        .ok_or_else(|| anyhow!("missing collection"))?;
    let data = opts
        .get("data")
        .filter(|d| d.is_object())
        .ok_or_else(|| anyhow!("missing data (an object)"))?;
    let token = auth_header().await?;
    // POST to the collection; Firestore auto-generates the id unless documentId given.
    let mut url = format!("{}/{}", fs_base(&project), urlencoding::encode(collection));
    if let Some(id) = opts["document"].as_str() {
        url.push_str(&format!("?documentId={}", urlencoding::encode(id)));
    }
    let body = json!({ "fields": fs_fields_encode(data) });
    let info: Value = client()
        .post(&url)
        .header("Authorization", token)
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let id = info["name"]
        .as_str()
        .and_then(|n| n.rsplit('/').next())
        .unwrap_or("");
    Ok(json!({ "collection": collection, "id": id }))
}

// ── Compute Engine ────────────────────────────────────────────────────────────

/// List Compute Engine instances in a zone. opts: zone (required). Returns
/// `{zone, instances: [{name, status, machine_type, zone}]}`.
async fn op_compute_list_instances(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let zone = opts["zone"]
        .as_str()
        .ok_or_else(|| anyhow!("missing zone"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://compute.googleapis.com/compute/v1/projects/{}/zones/{}/instances",
        urlencoding::encode(&project),
        urlencoding::encode(zone)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let instances: Vec<Value> = body["items"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|i| {
                    json!({
                        "name": i["name"].as_str().unwrap_or(""),
                        "status": i["status"].as_str().unwrap_or(""),
                        "machine_type": leaf_name(i["machineType"].as_str().unwrap_or("")),
                        "zone": leaf_name(i["zone"].as_str().unwrap_or("")),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"zone": zone, "instances": instances}))
}

/// Get one Compute Engine instance. opts: zone, instance (both required).
/// Returns the full instance resource.
async fn op_compute_get_instance(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let zone = opts["zone"]
        .as_str()
        .ok_or_else(|| anyhow!("missing zone"))?;
    let instance = opts["instance"]
        .as_str()
        .ok_or_else(|| anyhow!("missing instance"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://compute.googleapis.com/compute/v1/projects/{}/zones/{}/instances/{}",
        urlencoding::encode(&project),
        urlencoding::encode(zone),
        urlencoding::encode(instance)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(json!({"zone": zone, "instance": instance, "resource": body}))
}

/// Issue a lifecycle action (`start`/`stop`/`reset`) against an instance.
/// Shared by the start/stop/reset exports — the verb is the URL action suffix.
async fn compute_instance_action(opts: &Value, action: &str) -> Result<Value> {
    let project = resolve_project(opts).ok_or_else(|| anyhow!("missing project"))?;
    let zone = opts["zone"]
        .as_str()
        .ok_or_else(|| anyhow!("missing zone"))?;
    let instance = opts["instance"]
        .as_str()
        .ok_or_else(|| anyhow!("missing instance"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://compute.googleapis.com/compute/v1/projects/{}/zones/{}/instances/{}/{}",
        urlencoding::encode(&project),
        urlencoding::encode(zone),
        urlencoding::encode(instance),
        action
    );
    let info: Value = client()
        .post(&url)
        .header("Authorization", token)
        .header("Content-Length", "0")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(json!({
        "instance": instance,
        "action": action,
        "operation": info["name"].clone(),
        "status": info["status"].clone(),
    }))
}

/// Start a stopped Compute Engine instance. opts: zone, instance.
async fn op_compute_start_instance(opts: Value) -> Result<Value> {
    compute_instance_action(&opts, "start").await
}

/// Stop a running Compute Engine instance. opts: zone, instance.
async fn op_compute_stop_instance(opts: Value) -> Result<Value> {
    compute_instance_action(&opts, "stop").await
}

/// Hard-reset a Compute Engine instance. opts: zone, instance.
async fn op_compute_reset_instance(opts: Value) -> Result<Value> {
    compute_instance_action(&opts, "reset").await
}

/// List the Compute Engine zones available to the project. Returns
/// `{zones: [{name, region, status}]}`.
async fn op_compute_list_zones(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://compute.googleapis.com/compute/v1/projects/{}/zones",
        urlencoding::encode(&project)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let zones: Vec<Value> = body["items"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|z| {
                    json!({
                        "name": z["name"].as_str().unwrap_or(""),
                        "region": leaf_name(z["region"].as_str().unwrap_or("")),
                        "status": z["status"].as_str().unwrap_or(""),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"project": project, "zones": zones}))
}

/// List the Compute Engine regions available to the project (`regions.list`).
/// Returns `{regions: [{name, status}]}`.
async fn op_compute_list_regions(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://compute.googleapis.com/compute/v1/projects/{}/regions",
        urlencoding::encode(&project)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let regions: Vec<Value> = body["items"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|r| {
                    json!({
                        "name": r["name"].as_str().unwrap_or(""),
                        "status": r["status"].as_str().unwrap_or(""),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"project": project, "regions": regions}))
}

/// List the machine types available in a zone (`machineTypes.list`). opts: zone
/// (required). Returns `{zone, machine_types: [{name, guest_cpus, memory_mb,
/// description}]}`.
async fn op_compute_list_machine_types(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let zone = opts["zone"]
        .as_str()
        .ok_or_else(|| anyhow!("missing zone"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://compute.googleapis.com/compute/v1/projects/{}/zones/{}/machineTypes",
        urlencoding::encode(&project),
        urlencoding::encode(zone)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let types: Vec<Value> = body["items"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|m| {
                    json!({
                        "name": m["name"].as_str().unwrap_or(""),
                        "guest_cpus": m["guestCpus"].clone(),
                        "memory_mb": m["memoryMb"].clone(),
                        "description": m["description"].as_str().unwrap_or(""),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"zone": zone, "machine_types": types}))
}

/// List the persistent disks in a zone (`disks.list`). opts: zone (required).
/// Returns `{zone, disks: [{name, size_gb, type, status, zone}]}`.
async fn op_compute_list_disks(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let zone = opts["zone"]
        .as_str()
        .ok_or_else(|| anyhow!("missing zone"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://compute.googleapis.com/compute/v1/projects/{}/zones/{}/disks",
        urlencoding::encode(&project),
        urlencoding::encode(zone)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let disks: Vec<Value> = body["items"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|d| {
                    json!({
                        "name": d["name"].as_str().unwrap_or(""),
                        "size_gb": d["sizeGb"].as_str().unwrap_or("0"),
                        "type": leaf_name(d["type"].as_str().unwrap_or("")),
                        "status": d["status"].as_str().unwrap_or(""),
                        "zone": leaf_name(d["zone"].as_str().unwrap_or("")),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"zone": zone, "disks": disks}))
}

// ── Cloud Run ───────────────────────────────────────────────────────────────

/// List Cloud Run (v2) services in a region. opts: region (required). Returns
/// `{region, services: [{name, uri, latest_revision}]}`.
async fn op_run_list_services(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let region = opts["region"]
        .as_str()
        .ok_or_else(|| anyhow!("missing region"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://run.googleapis.com/v2/projects/{}/locations/{}/services",
        urlencoding::encode(&project),
        urlencoding::encode(region)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let services: Vec<Value> = body["services"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|s| {
                    json!({
                        "name": leaf_name(s["name"].as_str().unwrap_or("")),
                        "uri": s["uri"].clone(),
                        "latest_revision": leaf_name(s["latestReadyRevision"].as_str().unwrap_or("")),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"region": region, "services": services}))
}

/// Get one Cloud Run (v2) service. opts: region, service (both required).
async fn op_run_get_service(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let region = opts["region"]
        .as_str()
        .ok_or_else(|| anyhow!("missing region"))?;
    let service = opts["service"]
        .as_str()
        .ok_or_else(|| anyhow!("missing service"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://run.googleapis.com/v2/projects/{}/locations/{}/services/{}",
        urlencoding::encode(&project),
        urlencoding::encode(region),
        urlencoding::encode(service)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(json!({"region": region, "service": service, "resource": body}))
}

/// List the revisions of a Cloud Run (v2) service. opts: region, service.
/// Returns `{region, service, revisions: [{name, ...}]}`.
async fn op_run_list_revisions(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let region = opts["region"]
        .as_str()
        .ok_or_else(|| anyhow!("missing region"))?;
    let service = opts["service"]
        .as_str()
        .ok_or_else(|| anyhow!("missing service"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://run.googleapis.com/v2/projects/{}/locations/{}/services/{}/revisions",
        urlencoding::encode(&project),
        urlencoding::encode(region),
        urlencoding::encode(service)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let revisions: Vec<Value> = body["revisions"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|r| {
                    json!({
                        "name": leaf_name(r["name"].as_str().unwrap_or("")),
                        "create_time": r["createTime"].clone(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"region": region, "service": service, "revisions": revisions}))
}

// ── Cloud Functions ───────────────────────────────────────────────────────────

/// List Cloud Functions (v2) in a region (use `-` for all regions). opts:
/// region (default "-"). Returns `{region, functions: [{name, state, url}]}`.
async fn op_functions_list(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let region = opts["region"].as_str().unwrap_or("-");
    let token = auth_header().await?;
    let url = format!(
        "https://cloudfunctions.googleapis.com/v2/projects/{}/locations/{}/functions",
        urlencoding::encode(&project),
        urlencoding::encode(region)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let functions: Vec<Value> = body["functions"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|f| {
                    json!({
                        "name": leaf_name(f["name"].as_str().unwrap_or("")),
                        "state": f["state"].clone(),
                        "url": f["serviceConfig"]["uri"].clone(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"region": region, "functions": functions}))
}

/// Describe one Cloud Function (v2). opts: region, function (both required).
async fn op_functions_describe(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let region = opts["region"]
        .as_str()
        .ok_or_else(|| anyhow!("missing region"))?;
    let function = opts["function"]
        .as_str()
        .ok_or_else(|| anyhow!("missing function"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://cloudfunctions.googleapis.com/v2/projects/{}/locations/{}/functions/{}",
        urlencoding::encode(&project),
        urlencoding::encode(region),
        urlencoding::encode(function)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(json!({"region": region, "function": function, "resource": body}))
}

// ── Cloud Logging ─────────────────────────────────────────────────────────────

/// List recent log entries via `entries:list`. opts: filter (optional advanced
/// logs filter), page_size (default 50), order (default "desc"). Returns
/// `{entries: [{timestamp, severity, log_name, text_payload, json_payload}]}`.
async fn op_logging_list_entries(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let page_size = opts["page_size"].as_u64().unwrap_or(50);
    let order = match opts["order"].as_str() {
        Some("asc") => "timestamp asc",
        _ => "timestamp desc",
    };
    let token = auth_header().await?;
    let mut req = json!({
        "resourceNames": [format!("projects/{project}")],
        "pageSize": page_size,
        "orderBy": order,
    });
    if let Some(f) = opts["filter"].as_str() {
        req["filter"] = json!(f);
    }
    let body: Value = client()
        .post("https://logging.googleapis.com/v2/entries:list")
        .header("Authorization", token)
        .json(&req)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let entries: Vec<Value> = body["entries"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|e| {
                    json!({
                        "timestamp": e["timestamp"].clone(),
                        "severity": e["severity"].clone(),
                        "log_name": leaf_name(e["logName"].as_str().unwrap_or("")),
                        "text_payload": e["textPayload"].clone(),
                        "json_payload": e["jsonPayload"].clone(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"project": project, "entries": entries}))
}

/// Write a single log entry via `entries:write`. opts: log (required, the log
/// id), text (the text payload) OR json (a structured payload object),
/// severity (default "DEFAULT"). Returns `{written: true}`.
async fn op_logging_write_entry(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let log = opts["log"].as_str().ok_or_else(|| anyhow!("missing log"))?;
    let severity = opts["severity"].as_str().unwrap_or("DEFAULT");
    let mut entry = json!({
        "logName": format!("projects/{project}/logs/{}", urlencoding::encode(log)),
        "resource": { "type": "global" },
        "severity": severity,
    });
    if let Some(j) = opts.get("json").filter(|v| v.is_object()) {
        entry["jsonPayload"] = j.clone();
    } else if let Some(t) = opts["text"].as_str() {
        entry["textPayload"] = json!(t);
    } else {
        return Err(anyhow!("missing text or json payload"));
    }
    let token = auth_header().await?;
    let req = json!({ "entries": [entry] });
    client()
        .post("https://logging.googleapis.com/v2/entries:write")
        .header("Authorization", token)
        .json(&req)
        .send()
        .await?
        .error_for_status()?;
    Ok(json!({"log": log, "written": true}))
}

/// List the log ids that have entries in the project (`logs.list`). Returns
/// `{logs: [<log id>]}`.
async fn op_logging_list_logs(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://logging.googleapis.com/v2/projects/{}/logs",
        urlencoding::encode(&project)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let logs: Vec<String> = body["logNames"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|n| n.as_str().map(|s| leaf_name(s).to_string()))
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"project": project, "logs": logs}))
}

// ── Cloud Monitoring ──────────────────────────────────────────────────────────

/// List time series via `timeSeries.list`. opts: filter (required monitoring
/// filter, e.g. `metric.type="compute.googleapis.com/instance/cpu/utilization"`),
/// start (RFC3339), end (RFC3339). Returns `{time_series: [...]}`.
async fn op_monitoring_list_time_series(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let filter = opts["filter"]
        .as_str()
        .ok_or_else(|| anyhow!("missing filter"))?;
    let start = opts["start"]
        .as_str()
        .ok_or_else(|| anyhow!("missing start (RFC3339 interval start)"))?;
    let end = opts["end"]
        .as_str()
        .ok_or_else(|| anyhow!("missing end (RFC3339 interval end)"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://monitoring.googleapis.com/v3/projects/{}/timeSeries?filter={}&interval.startTime={}&interval.endTime={}",
        urlencoding::encode(&project),
        urlencoding::encode(filter),
        urlencoding::encode(start),
        urlencoding::encode(end)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(json!({
        "project": project,
        "time_series": body["timeSeries"].as_array().cloned().unwrap_or_default(),
    }))
}

/// List the metric descriptors visible to the project
/// (`metricDescriptors.list`). opts: filter (optional). Returns
/// `{descriptors: [{type, display_name, kind, value_type}]}`.
async fn op_monitoring_list_metric_descriptors(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let token = auth_header().await?;
    let mut url = format!(
        "https://monitoring.googleapis.com/v3/projects/{}/metricDescriptors",
        urlencoding::encode(&project)
    );
    if let Some(f) = opts["filter"].as_str() {
        url.push_str(&format!("?filter={}", urlencoding::encode(f)));
    }
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let descriptors: Vec<Value> = body["metricDescriptors"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|d| {
                    json!({
                        "type": d["type"].clone(),
                        "display_name": d["displayName"].clone(),
                        "kind": d["metricKind"].clone(),
                        "value_type": d["valueType"].clone(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"project": project, "descriptors": descriptors}))
}

// ── Secret Manager (list) ─────────────────────────────────────────────────────

/// List secret ids in the project (`secrets.list`). Returns
/// `{secrets: [<secret id>]}`.
async fn op_secret_list(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://secretmanager.googleapis.com/v1/projects/{}/secrets",
        urlencoding::encode(&project)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let secrets: Vec<String> = body["secrets"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|s| s["name"].as_str().map(|n| leaf_name(n).to_string()))
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"project": project, "secrets": secrets}))
}

// ── IAM ────────────────────────────────────────────────────────────────────────

/// Get the IAM policy for a Compute / generic resource by full
/// `getIamPolicy` URL. opts: url (required — the full `:getIamPolicy` endpoint),
/// or use the service-specific helpers. Returns `{policy}`.
async fn op_iam_get_policy(opts: Value) -> Result<Value> {
    let url = opts["url"]
        .as_str()
        .ok_or_else(|| anyhow!("missing url (full :getIamPolicy endpoint)"))?;
    let token = auth_header().await?;
    let policy: Value = client()
        .get(url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(json!({"policy": policy}))
}

/// Test which of a set of permissions the caller holds on a resource via a
/// full `:testIamPermissions` URL. opts: url (required), permissions (array,
/// required). Returns `{permissions: [<granted permission>]}`.
async fn op_iam_test_permissions(opts: Value) -> Result<Value> {
    let url = opts["url"]
        .as_str()
        .ok_or_else(|| anyhow!("missing url (full :testIamPermissions endpoint)"))?;
    let permissions = opts["permissions"]
        .as_array()
        .ok_or_else(|| anyhow!("missing permissions (array)"))?;
    let token = auth_header().await?;
    let body: Value = client()
        .post(url)
        .header("Authorization", token)
        .json(&json!({ "permissions": permissions }))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let granted: Vec<Value> = body["permissions"].as_array().cloned().unwrap_or_default();
    Ok(json!({"permissions": granted}))
}

/// List the service accounts in the project (`serviceAccounts.list`). Returns
/// `{service_accounts: [{email, display_name, unique_id}]}`.
async fn op_iam_list_service_accounts(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://iam.googleapis.com/v1/projects/{}/serviceAccounts",
        urlencoding::encode(&project)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let accounts: Vec<Value> = body["accounts"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|s| {
                    json!({
                        "email": s["email"].clone(),
                        "display_name": s["displayName"].clone(),
                        "unique_id": s["uniqueId"].clone(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"project": project, "service_accounts": accounts}))
}

/// Get one service account by email (`serviceAccounts.get`). opts: email
/// (required). Returns the service-account resource.
async fn op_iam_get_service_account(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let email = opts["email"]
        .as_str()
        .ok_or_else(|| anyhow!("missing email"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://iam.googleapis.com/v1/projects/{}/serviceAccounts/{}",
        urlencoding::encode(&project),
        urlencoding::encode(email)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(json!({"email": email, "resource": body}))
}

// ── BigQuery (datasets + tables) ───────────────────────────────────────────────

/// List datasets in the project (`datasets.list`). Returns
/// `{datasets: [<dataset id>]}`.
async fn op_bigquery_list_datasets(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets",
        urlencoding::encode(&project)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let datasets: Vec<String> = body["datasets"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|d| {
                    d["datasetReference"]["datasetId"]
                        .as_str()
                        .map(String::from)
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"project": project, "datasets": datasets}))
}

/// Get one dataset's metadata (`datasets.get`). opts: dataset (required).
async fn op_bigquery_get_dataset(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let dataset = opts["dataset"]
        .as_str()
        .ok_or_else(|| anyhow!("missing dataset"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}",
        urlencoding::encode(&project),
        urlencoding::encode(dataset)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(json!({"dataset": dataset, "resource": body}))
}

/// List tables in a dataset (`tables.list`). opts: dataset (required). Returns
/// `{dataset, tables: [<table id>]}`.
async fn op_bigquery_list_tables(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let dataset = opts["dataset"]
        .as_str()
        .ok_or_else(|| anyhow!("missing dataset"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables",
        urlencoding::encode(&project),
        urlencoding::encode(dataset)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let tables: Vec<String> = body["tables"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|t| t["tableReference"]["tableId"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"dataset": dataset, "tables": tables}))
}

/// Get one table's metadata (`tables.get`). opts: dataset, table (both required).
async fn op_bigquery_get_table(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let dataset = opts["dataset"]
        .as_str()
        .ok_or_else(|| anyhow!("missing dataset"))?;
    let table = opts["table"]
        .as_str()
        .ok_or_else(|| anyhow!("missing table"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/datasets/{}/tables/{}",
        urlencoding::encode(&project),
        urlencoding::encode(dataset),
        urlencoding::encode(table)
    );
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(json!({"dataset": dataset, "table": table, "resource": body}))
}

/// List recent BigQuery jobs in the project (`jobs.list`). opts: all_users
/// (default false — only the caller's jobs), max_results (default 50),
/// state_filter (one of "done"/"pending"/"running"). Returns
/// `{project, jobs: [{id, state, job_type, user_email}]}`.
async fn op_bigquery_list_jobs(opts: Value) -> Result<Value> {
    let project = resolve_project(&opts).ok_or_else(|| anyhow!("missing project"))?;
    let all_users = truthy(&opts["all_users"]);
    let max_results = opts["max_results"].as_u64().unwrap_or(50);
    let token = auth_header().await?;
    let mut url = format!(
        "https://bigquery.googleapis.com/bigquery/v2/projects/{}/jobs?allUsers={}&maxResults={}",
        urlencoding::encode(&project),
        all_users,
        max_results
    );
    if let Some(sf) = opts["state_filter"].as_str() {
        url.push_str(&format!("&stateFilter={}", urlencoding::encode(sf)));
    }
    let body: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let jobs: Vec<Value> = body["jobs"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|j| {
                    json!({
                        "id": j["jobReference"]["jobId"].as_str().unwrap_or(""),
                        "state": j["status"]["state"].as_str().unwrap_or(""),
                        "job_type": if j["statistics"]["query"].is_object() {
                            "query"
                        } else {
                            j["configuration"]["jobType"].as_str().unwrap_or("")
                        },
                        "user_email": j["user_email"].as_str().unwrap_or(""),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(json!({"project": project, "jobs": jobs}))
}

// ── GCS IAM ─────────────────────────────────────────────────────────────────

/// Get a bucket's IAM policy (`b/<bucket>/iam`). opts: bucket (required).
/// Returns `{bucket, policy}`.
async fn op_gcs_get_iam_policy(opts: Value) -> Result<Value> {
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/iam",
        urlencoding::encode(bucket)
    );
    let policy: Value = client()
        .get(&url)
        .header("Authorization", token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(json!({"bucket": bucket, "policy": policy}))
}

/// Set a bucket's IAM policy (`PUT b/<bucket>/iam`). opts: bucket (required),
/// policy (required — the full IAM policy object). Returns `{bucket, policy}`.
async fn op_gcs_set_iam_policy(opts: Value) -> Result<Value> {
    let bucket = opts["bucket"]
        .as_str()
        .ok_or_else(|| anyhow!("missing bucket"))?;
    let policy = opts
        .get("policy")
        .filter(|p| p.is_object())
        .ok_or_else(|| anyhow!("missing policy (an object)"))?;
    let token = auth_header().await?;
    let url = format!(
        "https://storage.googleapis.com/storage/v1/b/{}/iam",
        urlencoding::encode(bucket)
    );
    let applied: Value = client()
        .put(&url)
        .header("Authorization", token)
        .json(policy)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(json!({"bucket": bucket, "policy": applied}))
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call_async<F, Fut>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Fut,
    Fut: std::future::Future<Output = Result<Value>>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let fut = handler(input);
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| rt().block_on(fut)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-gcp handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── pure helpers (no GCP) ────────────────────────────────────────────────────

/// Parse a `gs://bucket/object` URI into `{bucket, object}` (object is null
/// when only a bucket is given). Pure.
fn op_parse_gs_uri(opts: Value) -> Result<Value> {
    let uri = opts
        .get("uri")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing uri"))?;
    let rest = uri
        .strip_prefix("gs://")
        .ok_or_else(|| anyhow!("not a gs:// URI: {uri}"))?;
    let (bucket, object) = match rest.split_once('/') {
        Some((b, o)) => (b, json!(o)),
        None => (rest, Value::Null),
    };
    if bucket.is_empty() {
        return Err(anyhow!("gs URI missing bucket: {uri}"));
    }
    Ok(json!({"bucket": bucket, "object": object}))
}

/// Build a `gs://bucket/object` URI from parts. opts: bucket (required), object
/// (optional — omitted yields the bare `gs://bucket`). Leading slashes on the
/// object are trimmed so callers can pass either `obj` or `/obj`. Inverse of
/// `parse_gs_uri`. Pure.
fn op_build_gs_uri(opts: Value) -> Result<Value> {
    let bucket = opts
        .get("bucket")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing bucket"))?;
    if bucket.is_empty() {
        return Err(anyhow!("bucket must not be empty"));
    }
    let object = opts
        .get("object")
        .and_then(Value::as_str)
        .map(|o| o.trim_start_matches('/'))
        .filter(|o| !o.is_empty());
    let uri = match object {
        Some(o) => format!("gs://{bucket}/{o}"),
        None => format!("gs://{bucket}"),
    };
    Ok(json!({"uri": uri}))
}

/// Convert a `gs://bucket/object` URI to its path-style public download URL
/// `https://storage.googleapis.com/bucket/object`. A bucket-only URI yields the
/// bucket endpoint. opts: uri (required). Returns `{url, bucket, object}`. Pure.
fn op_gs_uri_to_url(opts: Value) -> Result<Value> {
    let uri = opts
        .get("uri")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing uri"))?;
    let rest = uri
        .strip_prefix("gs://")
        .ok_or_else(|| anyhow!("not a gs:// URI: {uri}"))?;
    let (bucket, object) = match rest.split_once('/') {
        Some((b, o)) if !o.is_empty() => (b, Some(o)),
        Some((b, _)) => (b, None),
        None => (rest, None),
    };
    if bucket.is_empty() {
        return Err(anyhow!("gs URI missing bucket: {uri}"));
    }
    let url = match object {
        Some(o) => format!("https://storage.googleapis.com/{bucket}/{o}"),
        None => format!("https://storage.googleapis.com/{bucket}"),
    };
    Ok(json!({
        "url": url,
        "bucket": bucket,
        "object": object.map(|o| json!(o)).unwrap_or(Value::Null),
    }))
}

/// Convert a GCS download URL back to its `gs://bucket/object` URI. Accepts both
/// path-style `https://storage.googleapis.com/bucket/object` and
/// virtual-hosted-style `https://bucket.storage.googleapis.com/object` (the
/// `commondatastorage.googleapis.com` alias too). opts: url (required). Returns
/// `{uri, bucket, object}`. Inverse of `gs_uri_to_url`. Pure.
fn op_url_to_gs_uri(opts: Value) -> Result<Value> {
    let url = opts
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing url"))?;
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .ok_or_else(|| anyhow!("not an http(s) URL: {url}"))?;
    const HOSTS: &[&str] = &["storage.googleapis.com", "commondatastorage.googleapis.com"];
    let (host, path) = rest.split_once('/').unwrap_or((rest, ""));
    let (bucket, object) = if HOSTS.contains(&host) {
        // Path-style: host is the API host, bucket is the first path segment.
        match path.split_once('/') {
            Some((b, o)) if !o.is_empty() => (b.to_string(), Some(o.to_string())),
            Some((b, _)) => (b.to_string(), None),
            None => (path.to_string(), None),
        }
    } else if let Some(b) = HOSTS
        .iter()
        .find_map(|h| host.strip_suffix(&format!(".{h}")))
    {
        // Virtual-hosted-style: bucket is the host label, object is the path.
        let obj = (!path.is_empty()).then(|| path.to_string());
        (b.to_string(), obj)
    } else {
        return Err(anyhow!("not a GCS URL: {url}"));
    };
    if bucket.is_empty() {
        return Err(anyhow!("GCS URL missing bucket: {url}"));
    }
    let uri = match &object {
        Some(o) => format!("gs://{bucket}/{o}"),
        None => format!("gs://{bucket}"),
    };
    Ok(json!({
        "uri": uri,
        "bucket": bucket,
        "object": object.map(|o| json!(o)).unwrap_or(Value::Null),
    }))
}

/// Parse a GCP resource name `collection/id/collection/id…` (e.g.
/// `projects/p/topics/t`) into its `parts` plus a `pairs` map keyed by each
/// collection segment. An odd trailing segment is returned as `trailing`. Pure.
fn op_parse_resource_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .or_else(|| opts.get("resource"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let parts: Vec<&str> = name.split('/').filter(|s| !s.is_empty()).collect();
    if parts.is_empty() {
        return Err(anyhow!("empty resource name"));
    }
    let mut pairs = serde_json::Map::new();
    let mut i = 0;
    while i + 1 < parts.len() {
        pairs.insert(parts[i].to_string(), json!(parts[i + 1]));
        i += 2;
    }
    let trailing = if parts.len() % 2 == 1 {
        json!(parts[parts.len() - 1])
    } else {
        Value::Null
    };
    Ok(json!({
        "parts": parts,
        "pairs": Value::Object(pairs),
        "trailing": trailing,
    }))
}

/// The parent of a GCP relative resource name — the hierarchy "dirname" that
/// drops the trailing `{collection}/{id}` pair. `projects/p/locations/l/clusters/c`
/// → parent `projects/p/locations/l`, leaf collection `clusters`, id `c`. A
/// top-level resource (`projects/p`) has an empty parent and `has_parent` false.
/// A relative resource name is a sequence of collection/id pairs, so an odd
/// segment count (a dangling collection without an id) is rejected. Useful for
/// walking IAM inheritance / resource ancestry. opts: `name` (or `resource`,
/// required). Returns `{name, parent, has_parent, collection, id}`. Pure.
fn op_resource_name_parent(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .or_else(|| opts.get("resource"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let parts: Vec<&str> = name.split('/').filter(|s| !s.is_empty()).collect();
    if parts.len() < 2 || !parts.len().is_multiple_of(2) {
        return Err(anyhow!(
            "not a relative resource name (expected `collection/id` pairs): `{name}`"
        ));
    }
    let collection = parts[parts.len() - 2];
    let id = parts[parts.len() - 1];
    let parent_parts = &parts[..parts.len() - 2];
    let parent = parent_parts.join("/");
    Ok(json!({
        "name": name,
        "parent": parent,
        "has_parent": !parent_parts.is_empty(),
        "collection": collection,
        "id": id,
    }))
}

/// Assemble a GCP resource name `collection/id/collection/id…` from parts — the
/// inverse of `parse_resource_name`. Accepts either a flat `parts` array (the
/// exact inverse of the parsed `parts`) or ordered `pairs` — each a
/// `[collection, id]` array or `{collection, id}` object — plus an optional
/// `trailing` segment. Segments must be non-empty and contain no `/`. Returns
/// `{name}`. Pure.
fn op_build_resource_name(opts: Value) -> Result<Value> {
    let check = |s: &str| -> Result<String> {
        if s.is_empty() || s.contains('/') {
            return Err(anyhow!("invalid segment `{s}` (non-empty, no `/`)"));
        }
        Ok(s.to_string())
    };
    // Exact inverse: a flat `parts` list joined with `/`.
    if let Some(parts) = opts.get("parts").and_then(Value::as_array) {
        if parts.is_empty() {
            return Err(anyhow!("parts must not be empty"));
        }
        let segs: Vec<String> = parts
            .iter()
            .map(|p| {
                p.as_str()
                    .ok_or_else(|| anyhow!("parts entries must be strings"))
                    .and_then(check)
            })
            .collect::<Result<_>>()?;
        return Ok(json!({ "name": segs.join("/") }));
    }
    // Ordered collection/id pairs plus an optional trailing segment.
    let pairs = opts
        .get("pairs")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("missing parts or pairs"))?;
    let mut segs: Vec<String> = Vec::new();
    for p in pairs {
        let (c, id) = match p {
            Value::Array(a) if a.len() == 2 => (a[0].as_str(), a[1].as_str()),
            Value::Object(_) => (
                p.get("collection").and_then(Value::as_str),
                p.get("id").and_then(Value::as_str),
            ),
            _ => {
                return Err(anyhow!(
                    "each pair must be [collection, id] or {{collection, id}}"
                ))
            }
        };
        segs.push(check(c.ok_or_else(|| anyhow!("pair missing collection"))?)?);
        segs.push(check(id.ok_or_else(|| anyhow!("pair missing id"))?)?);
    }
    if let Some(t) = opts.get("trailing").and_then(Value::as_str) {
        segs.push(check(t)?);
    }
    if segs.is_empty() {
        return Err(anyhow!("no segments to build"));
    }
    Ok(json!({ "name": segs.join("/") }))
}

/// Parse a BigQuery fully-qualified table reference into `{project, dataset,
/// table, legacy}`. Standard SQL dots everything (`project.dataset.table`);
/// legacy SQL / the bq CLI put a `:` after the project (`project:dataset.table`).
/// A project-less `dataset.table` is accepted (project is `null`). The dataset
/// and table segments are always required and may not be empty. opts: `table_ref`
/// (or `value`, required). Returns `{table_ref, project, dataset, table, legacy}`.
/// Pure.
fn op_parse_table_ref(opts: Value) -> Result<Value> {
    let r = opts
        .get("table_ref")
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing table_ref"))?;
    // Legacy `project:dataset.table` has exactly one `:` splitting off the project.
    let (project, rest, legacy) = if let Some((p, rest)) = r.split_once(':') {
        if rest.contains(':') {
            return Err(anyhow!("invalid table ref `{r}` (more than one `:`)"));
        }
        (Some(p), rest, true)
    } else {
        (None, r, false)
    };
    let dot_parts: Vec<&str> = rest.split('.').collect();
    let (project, dataset, table) = match (project, dot_parts.as_slice()) {
        // Legacy: project came from the `:`, so the remainder is dataset.table.
        (Some(p), [d, t]) => (Some(p), *d, *t),
        // Standard dotted forms.
        (None, [p, d, t]) => (Some(*p), *d, *t),
        (None, [d, t]) => (None, *d, *t),
        _ => {
            return Err(anyhow!(
                "invalid table ref `{r}` (want [project.|project:]dataset.table)"
            ))
        }
    };
    if dataset.is_empty() || table.is_empty() || project == Some("") {
        return Err(anyhow!("table ref `{r}` has an empty segment"));
    }
    Ok(json!({
        "table_ref": r,
        "project": project,
        "dataset": dataset,
        "table": table,
        "legacy": legacy,
    }))
}

/// Assemble a BigQuery table reference from parts — the inverse of
/// `parse_table_ref`. With a `project`, `legacy` (default false) selects the
/// separator: standard SQL dots everything (`project.dataset.table`) while legacy
/// uses a `:` after the project (`project:dataset.table`). Without a project the
/// result is `dataset.table` (and `legacy` then requires a project, so it errors).
/// No segment may contain a `.` or `:`. opts: `dataset` (required), `table`
/// (required), `project` (optional), `legacy` (optional). Returns
/// `{table_ref, project, dataset, table, legacy}`. Pure.
fn op_build_table_ref(opts: Value) -> Result<Value> {
    let dataset = opts
        .get("dataset")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing dataset"))?;
    let table = opts
        .get("table")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing table"))?;
    let project = opts
        .get("project")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let legacy = match opts.get("legacy") {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_i64().is_some_and(|x| x != 0),
        Some(Value::String(s)) => s == "1" || s.eq_ignore_ascii_case("true"),
        _ => false,
    };
    for (name, seg) in [("dataset", dataset), ("table", table)]
        .into_iter()
        .chain(project.map(|p| ("project", p)))
    {
        if seg.contains('.') || seg.contains(':') {
            return Err(anyhow!("{name} must not contain `.` or `:`: {seg}"));
        }
    }
    let table_ref = match (project, legacy) {
        (Some(p), true) => format!("{p}:{dataset}.{table}"),
        (Some(p), false) => format!("{p}.{dataset}.{table}"),
        (None, true) => return Err(anyhow!("legacy form requires a project")),
        (None, false) => format!("{dataset}.{table}"),
    };
    Ok(json!({
        "table_ref": table_ref,
        "project": project,
        "dataset": dataset,
        "table": table,
        "legacy": legacy,
    }))
}

/// Validate a GCS bucket name against Google's rules: 3–63 chars of
/// `[a-z0-9-._]`, start/end alphanumeric, not an IPv4 literal, no `goog`
/// prefix, no `google` substring. (Note: unlike S3, underscores are allowed.)
/// Returns `{valid, reason}`. Pure.
fn op_valid_bucket_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let bytes = name.as_bytes();
    let reason: Option<&str> = if name.len() < 3 || name.len() > 63 {
        Some("must be 3-63 characters")
    } else if !name.bytes().all(|b| {
        b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_' || b == b'.'
    }) {
        Some("only lowercase letters, numbers, hyphens, underscores, and dots")
    } else if !bytes[0].is_ascii_alphanumeric() || !bytes[bytes.len() - 1].is_ascii_alphanumeric() {
        Some("must start and end with a letter or number")
    } else if name.parse::<std::net::Ipv4Addr>().is_ok() {
        Some("must not be formatted as an IP address")
    } else if name.starts_with("goog") {
        Some("must not begin with the `goog` prefix")
    } else if name.contains("google") {
        Some("must not contain `google`")
    } else {
        None
    };
    Ok(json!({"name": name, "valid": reason.is_none(), "reason": reason}))
}

/// Validate a GCP project ID against Google's rules: 6–30 characters of
/// lowercase letters, digits, and hyphens; must start with a lowercase letter
/// and must not end with a hyphen. Distinct from a bucket name. Returns
/// `{id, valid, reason}`. Pure.
fn op_valid_project_id(opts: Value) -> Result<Value> {
    let id = opts
        .get("id")
        .or_else(|| opts.get("project_id"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing id"))?;
    let bytes = id.as_bytes();
    let reason: Option<&str> = if id.len() < 6 || id.len() > 30 {
        Some("must be 6-30 characters")
    } else if !bytes[0].is_ascii_lowercase() {
        Some("must start with a lowercase letter")
    } else if id.ends_with('-') {
        Some("must not end with a hyphen")
    } else if !id
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        Some("only lowercase letters, digits, and hyphens")
    } else {
        None
    };
    Ok(json!({"id": id, "valid": reason.is_none(), "reason": reason}))
}

/// Validate a Cloud Storage object name against GCS's hard requirements
/// (cloud.google.com/storage/docs/objects): 1–1024 bytes when UTF-8 encoded; no
/// Carriage Return or Line Feed; it cannot be exactly `.` or `..`; and it cannot
/// start with `.well-known/acme-challenge/`. Characters GCS only *recommends*
/// avoiding (control chars, `#`, `[`, `]`, `*`, `?`, …) are NOT rejected — only
/// the hard rules. Distinct from `valid_bucket_name`. opts: `name` (required).
/// Returns `{name, valid, reason}`. Pure.
fn op_valid_object_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let reason: Option<&str> = if name.is_empty() {
        Some("must be at least 1 byte")
    } else if name.len() > 1024 {
        Some("must be at most 1024 bytes when UTF-8 encoded")
    } else if name.contains(['\r', '\n']) {
        Some("must not contain a carriage return or line feed")
    } else if name == "." || name == ".." {
        Some("must not be `.` or `..`")
    } else if name.starts_with(".well-known/acme-challenge/") {
        Some("must not start with `.well-known/acme-challenge/`")
    } else {
        None
    };
    Ok(json!({"name": name, "valid": reason.is_none(), "reason": reason}))
}

/// Derive the GCP region from a zone name. GCP zones are `<region>-<letter>`
/// (e.g. `us-central1-a` → `us-central1`, `europe-west4-b` → `europe-west4`),
/// so the region is the zone with its trailing single-letter suffix removed.
/// opts: `zone` (required). Returns `{zone, region, zone_letter}`; errors if the
/// zone has no `-` or the suffix isn't a single ASCII letter. Pure.
fn op_region_for_zone(opts: Value) -> Result<Value> {
    let zone = opts
        .get("zone")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing zone"))?;
    let (region, letter) = zone
        .rsplit_once('-')
        .ok_or_else(|| anyhow!("not a GCP zone (want <region>-<letter>): {zone}"))?;
    if region.is_empty() {
        return Err(anyhow!("zone `{zone}` has an empty region part"));
    }
    if letter.len() != 1 || !letter.as_bytes()[0].is_ascii_alphabetic() {
        return Err(anyhow!(
            "zone `{zone}` must end with a single-letter zone suffix"
        ));
    }
    Ok(json!({"zone": zone, "region": region, "zone_letter": letter}))
}

/// Validate a GCP service-account ID — the `<id>` part before the `@` in a
/// service-account email. Per the IAM `accountId` rules it is 6-30 characters,
/// RFC1035-compliant: only lowercase letters, digits and hyphens, must start with
/// a lowercase letter, and must not end with a hyphen. The IAM member of the
/// `valid_*` family, mirroring its `{valid, reason}` shape. opts: `id` (or
/// `account_id`/`name`/`value`, required). Returns `{id, valid, reason}`. Pure.
fn op_valid_service_account_id(opts: Value) -> Result<Value> {
    let id = opts
        .get("id")
        .or_else(|| opts.get("account_id"))
        .or_else(|| opts.get("name"))
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing id"))?;
    let bytes = id.as_bytes();
    let len = bytes.len();
    let reason: Option<&str> = if !(6..=30).contains(&len) {
        Some("must be 6-30 characters")
    } else if !id
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        Some("only lowercase letters, digits, and hyphens")
    } else if !bytes[0].is_ascii_lowercase() {
        Some("must start with a lowercase letter")
    } else if bytes[len - 1] == b'-' {
        Some("must not end with a hyphen")
    } else {
        None
    };
    Ok(json!({"id": id, "valid": reason.is_none(), "reason": reason}))
}

/// Validate a GCP Secret Manager secret ID per the Create-a-secret reference: 1
/// to 255 characters of letters (upper or lower case), numerals, hyphens (`-`)
/// and underscores (`_`). Unlike a service-account ID there is no leading-letter
/// or trailing-hyphen restriction. The Secret Manager member of the `valid_*`
/// family. opts: `id` (or `secret_id`/`name`/`value`, required). Returns `{id,
/// valid, reason}`. Pure.
fn op_valid_secret_id(opts: Value) -> Result<Value> {
    let id = opts
        .get("id")
        .or_else(|| opts.get("secret_id"))
        .or_else(|| opts.get("name"))
        .or_else(|| opts.get("value"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing id"))?;
    let reason: Option<&str> = if id.is_empty() {
        Some("must not be empty")
    } else if id.chars().count() > 255 {
        Some("must be at most 255 characters")
    } else if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        Some("only letters, numerals, hyphens, and underscores")
    } else {
        None
    };
    Ok(json!({"id": id, "valid": reason.is_none(), "reason": reason}))
}

/// Validate a GCP resource label `key`/`value` pair against the Resource Manager
/// rules: a key is 1-63 characters, must start with a lowercase letter (or an
/// international character), and contains only lowercase letters, digits, `_`,
/// and `-`; a value is 0-63 characters of the same charset and may be empty.
/// opts: `key` (required), `value` (optional, defaults to empty). Returns
/// `{key, value, valid, reason}`. Pure.
fn op_valid_label(opts: Value) -> Result<Value> {
    let key = opts
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing key"))?;
    let value = opts.get("value").and_then(Value::as_str).unwrap_or("");
    // International (non-ASCII) characters are allowed; only ASCII uppercase and
    // ASCII punctuation outside `_-` are disallowed.
    let char_ok = |c: char| {
        c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-' || !c.is_ascii()
    };
    let start_ok = |c: char| c.is_ascii_lowercase() || !c.is_ascii();
    let key_chars = key.chars().count();
    let first = key.chars().next();
    let reason: Option<&str> = if key.is_empty() {
        Some("key must not be empty")
    } else if key_chars > 63 {
        Some("key must be at most 63 characters")
    } else if !first.map(start_ok).unwrap_or(false) {
        Some("key must start with a lowercase letter")
    } else if !key.chars().all(char_ok) {
        Some("key may contain only lowercase letters, digits, '_', and '-'")
    } else if value.chars().count() > 63 {
        Some("value must be at most 63 characters")
    } else if !value.chars().all(char_ok) {
        Some("value may contain only lowercase letters, digits, '_', and '-'")
    } else {
        None
    };
    Ok(json!({"key": key, "value": value, "valid": reason.is_none(), "reason": reason}))
}

/// Validate a Pub/Sub resource ID (topic, subscription, schema, or snapshot)
/// against Google's naming guidelines
/// (cloud.google.com/pubsub/docs/pubsub-basics): 3–255 characters; must start
/// with a letter; may not begin with the string `goog`; and may contain only
/// letters `[A-Za-z]`, digits `[0-9]`, and `- _ . ~ + %`. The same rule governs
/// all four resource kinds. opts: `id` (or `name`). Returns `{id, valid,
/// reason}`. Pure.
fn op_valid_pubsub_id(opts: Value) -> Result<Value> {
    let id = opts
        .get("id")
        .or_else(|| opts.get("name"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing id"))?;
    let len = id.chars().count();
    let char_ok =
        |c: char| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~' | '+' | '%');
    let reason: Option<&str> = if !(3..=255).contains(&len) {
        Some("must be 3-255 characters")
    } else if !id.chars().next().is_some_and(|c| c.is_ascii_alphabetic()) {
        Some("must start with a letter")
    } else if id.starts_with("goog") {
        Some("must not begin with `goog`")
    } else if !id.chars().all(char_ok) {
        Some("only letters, digits, and '-', '_', '.', '~', '+', '%'")
    } else {
        None
    };
    Ok(json!({"id": id, "valid": reason.is_none(), "reason": reason}))
}

/// Validate a BigQuery dataset ID against Google's naming rules
/// (cloud.google.com/bigquery/docs/datasets): up to 1,024 characters of letters
/// (upper or lower case), digits, and underscores only — no spaces or special
/// characters like `-`, `&`, `@`, `%`. Dataset IDs are case-sensitive. A leading
/// underscore is allowed; it just marks a hidden dataset. opts: `id` (or `name`).
/// Returns `{id, valid, reason}`. Pure.
fn op_valid_dataset_id(opts: Value) -> Result<Value> {
    let id = opts
        .get("id")
        .or_else(|| opts.get("name"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing id"))?;
    let reason: Option<&str> = if id.is_empty() {
        Some("must not be empty")
    } else if id.chars().count() > 1024 {
        Some("must be at most 1024 characters")
    } else if !id.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Some("only letters, digits, and underscores")
    } else {
        None
    };
    Ok(json!({"id": id, "valid": reason.is_none(), "reason": reason}))
}

/// Validate a BigQuery table ID — the companion of `valid_dataset_id`, but with
/// the broader table-name rules: at most 1,024 UTF-8 bytes (not characters,
/// unlike a dataset ID) and Unicode letters/numbers plus underscores, dashes,
/// and spaces (the common members of the documented L/M/N/Pc/Pd/Zs categories).
/// opts: `id` (or `name`, required). Returns `{id, valid, reason, bytes}` where
/// `bytes` is the UTF-8 length. Pure.
fn op_valid_table_id(opts: Value) -> Result<Value> {
    let id = opts
        .get("id")
        .or_else(|| opts.get("name"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing id"))?;
    let reason: Option<&str> = if id.is_empty() {
        Some("must not be empty")
    } else if id.len() > 1024 {
        Some("UTF-8 encoding must be at most 1024 bytes")
    } else if !id
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == ' ')
    {
        Some("only letters, numbers, underscores, dashes, and spaces")
    } else {
        None
    };
    Ok(json!({"id": id, "valid": reason.is_none(), "reason": reason, "bytes": id.len()}))
}

// ── exports ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn gcp__pkg_version(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |_| async {
        Ok(json!({"version": env!("CARGO_PKG_VERSION")}))
    })
}

#[no_mangle]
pub extern "C" fn gcp__identity(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_identity)
}

#[no_mangle]
pub extern "C" fn gcp__gcs_list_buckets(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_gcs_list_buckets)
}

#[no_mangle]
pub extern "C" fn gcp__gcs_list_objects(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_gcs_list_objects)
}

#[no_mangle]
pub extern "C" fn gcp__gcs_get_object(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_gcs_get_object)
}

#[no_mangle]
pub extern "C" fn gcp__gcs_put_object(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_gcs_put_object)
}

#[no_mangle]
pub extern "C" fn gcp__gcs_delete_object(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_gcs_delete_object)
}

#[no_mangle]
pub extern "C" fn gcp__pubsub_publish(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_pubsub_publish)
}

#[no_mangle]
pub extern "C" fn gcp__pubsub_pull(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_pubsub_pull)
}

#[no_mangle]
pub extern "C" fn gcp__pubsub_ack(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_pubsub_ack)
}

#[no_mangle]
pub extern "C" fn gcp__gcs_head_object(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_gcs_head_object)
}

#[no_mangle]
pub extern "C" fn gcp__gcs_copy_object(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_gcs_copy_object)
}

#[no_mangle]
pub extern "C" fn gcp__pubsub_list_topics(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_pubsub_list_topics)
}

#[no_mangle]
pub extern "C" fn gcp__pubsub_list_subscriptions(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_pubsub_list_subscriptions)
}

#[no_mangle]
pub extern "C" fn gcp__pubsub_create_topic(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_pubsub_create_topic)
}

#[no_mangle]
pub extern "C" fn gcp__pubsub_create_subscription(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_pubsub_create_subscription)
}

#[no_mangle]
pub extern "C" fn gcp__secret_access(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_secret_access)
}

#[no_mangle]
pub extern "C" fn gcp__secret_create(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_secret_create)
}

#[no_mangle]
pub extern "C" fn gcp__secret_add_version(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_secret_add_version)
}

#[no_mangle]
pub extern "C" fn gcp__pubsub_delete_topic(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_pubsub_delete_topic)
}

#[no_mangle]
pub extern "C" fn gcp__pubsub_delete_subscription(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_pubsub_delete_subscription)
}

#[no_mangle]
pub extern "C" fn gcp__bigquery_query(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_bigquery_query)
}

#[no_mangle]
pub extern "C" fn gcp__bigquery_insert(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_bigquery_insert)
}

#[no_mangle]
pub extern "C" fn gcp__gcs_compose(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_gcs_compose)
}

#[no_mangle]
pub extern "C" fn gcp__firestore_get(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_firestore_get)
}

#[no_mangle]
pub extern "C" fn gcp__firestore_set(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_firestore_set)
}

#[no_mangle]
pub extern "C" fn gcp__firestore_delete(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_firestore_delete)
}

#[no_mangle]
pub extern "C" fn gcp__firestore_list(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_firestore_list)
}

#[no_mangle]
pub extern "C" fn gcp__firestore_query(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_firestore_query)
}

#[no_mangle]
pub extern "C" fn gcp__firestore_create(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_firestore_create)
}

#[no_mangle]
pub extern "C" fn gcp__parse_gs_uri(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_gs_uri(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__build_gs_uri(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_gs_uri(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__gs_uri_to_url(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_gs_uri_to_url(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__url_to_gs_uri(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_url_to_gs_uri(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__parse_resource_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_resource_name(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__resource_name_parent(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_resource_name_parent(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__build_resource_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_resource_name(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__parse_table_ref(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_table_ref(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__build_table_ref(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_table_ref(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__valid_bucket_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_bucket_name(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__valid_project_id(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_project_id(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__valid_object_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_object_name(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__region_for_zone(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_region_for_zone(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__valid_label(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_label(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__valid_pubsub_id(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_pubsub_id(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__valid_service_account_id(args: *const c_char) -> *const c_char {
    ffi_call_async(
        args,
        |opts| async move { op_valid_service_account_id(opts) },
    )
}

#[no_mangle]
pub extern "C" fn gcp__valid_secret_id(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_secret_id(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__valid_dataset_id(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_dataset_id(opts) })
}

#[no_mangle]
pub extern "C" fn gcp__valid_table_id(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_table_id(opts) })
}

// ── new-service exports ───────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn gcp__compute_list_instances(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_compute_list_instances)
}

#[no_mangle]
pub extern "C" fn gcp__compute_get_instance(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_compute_get_instance)
}

#[no_mangle]
pub extern "C" fn gcp__compute_start_instance(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_compute_start_instance)
}

#[no_mangle]
pub extern "C" fn gcp__compute_stop_instance(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_compute_stop_instance)
}

#[no_mangle]
pub extern "C" fn gcp__compute_reset_instance(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_compute_reset_instance)
}

#[no_mangle]
pub extern "C" fn gcp__compute_list_zones(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_compute_list_zones)
}

#[no_mangle]
pub extern "C" fn gcp__run_list_services(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_run_list_services)
}

#[no_mangle]
pub extern "C" fn gcp__run_get_service(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_run_get_service)
}

#[no_mangle]
pub extern "C" fn gcp__run_list_revisions(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_run_list_revisions)
}

#[no_mangle]
pub extern "C" fn gcp__functions_list(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_functions_list)
}

#[no_mangle]
pub extern "C" fn gcp__functions_describe(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_functions_describe)
}

#[no_mangle]
pub extern "C" fn gcp__logging_list_entries(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_logging_list_entries)
}

#[no_mangle]
pub extern "C" fn gcp__logging_write_entry(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_logging_write_entry)
}

#[no_mangle]
pub extern "C" fn gcp__logging_list_logs(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_logging_list_logs)
}

#[no_mangle]
pub extern "C" fn gcp__monitoring_list_time_series(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_monitoring_list_time_series)
}

#[no_mangle]
pub extern "C" fn gcp__monitoring_list_metric_descriptors(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_monitoring_list_metric_descriptors)
}

#[no_mangle]
pub extern "C" fn gcp__secret_list(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_secret_list)
}

#[no_mangle]
pub extern "C" fn gcp__iam_get_policy(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_iam_get_policy)
}

#[no_mangle]
pub extern "C" fn gcp__iam_test_permissions(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_iam_test_permissions)
}

#[no_mangle]
pub extern "C" fn gcp__iam_list_service_accounts(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_iam_list_service_accounts)
}

#[no_mangle]
pub extern "C" fn gcp__iam_get_service_account(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_iam_get_service_account)
}

#[no_mangle]
pub extern "C" fn gcp__bigquery_list_datasets(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_bigquery_list_datasets)
}

#[no_mangle]
pub extern "C" fn gcp__bigquery_get_dataset(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_bigquery_get_dataset)
}

#[no_mangle]
pub extern "C" fn gcp__bigquery_list_tables(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_bigquery_list_tables)
}

#[no_mangle]
pub extern "C" fn gcp__bigquery_get_table(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_bigquery_get_table)
}

#[no_mangle]
pub extern "C" fn gcp__gcs_get_iam_policy(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_gcs_get_iam_policy)
}

#[no_mangle]
pub extern "C" fn gcp__gcs_set_iam_policy(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_gcs_set_iam_policy)
}

#[no_mangle]
pub extern "C" fn gcp__compute_list_regions(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_compute_list_regions)
}

#[no_mangle]
pub extern "C" fn gcp__compute_list_machine_types(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_compute_list_machine_types)
}

#[no_mangle]
pub extern "C" fn gcp__compute_list_disks(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_compute_list_disks)
}

#[no_mangle]
pub extern "C" fn gcp__pubsub_get_topic(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_pubsub_get_topic)
}

#[no_mangle]
pub extern "C" fn gcp__pubsub_topic_subscriptions(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_pubsub_topic_subscriptions)
}

#[no_mangle]
pub extern "C" fn gcp__secret_list_versions(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_secret_list_versions)
}

#[no_mangle]
pub extern "C" fn gcp__secret_delete(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_secret_delete)
}

#[no_mangle]
pub extern "C" fn gcp__bigquery_list_jobs(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_bigquery_list_jobs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Firestore's typed-value encoding is the load-bearing translation for the
    /// Firestore ops — a wrong tag silently writes/reads the wrong type. Pin the
    /// scalar mappings and that integers travel as strings (Firestore quirk).
    #[test]
    fn fs_encode_scalars_use_correct_typed_tags() {
        assert_eq!(fs_encode(&json!("x")), json!({"stringValue": "x"}));
        assert_eq!(fs_encode(&json!(7)), json!({"integerValue": "7"}));
        assert_eq!(fs_encode(&json!(1.5)), json!({"doubleValue": 1.5}));
        assert_eq!(fs_encode(&json!(true)), json!({"booleanValue": true}));
        assert_eq!(fs_encode(&Value::Null), json!({"nullValue": null}));
    }

    /// A nested object/array round-trips through encode→decode unchanged
    /// (including the integer-as-string normalization back to a JSON integer).
    #[test]
    fn fs_encode_decode_round_trips_nested() {
        let original = json!({
            "name": "ada",
            "age": 36,
            "scores": [1, 2.5, "z"],
            "meta": { "active": true, "tags": ["x", "y"] },
        });
        let encoded = fs_encode(&original);
        // Spot-check the wire shape is the Firestore mapValue form.
        assert!(encoded["mapValue"]["fields"]["name"]["stringValue"] == json!("ada"));
        assert!(encoded["mapValue"]["fields"]["age"]["integerValue"] == json!("36"));
        // Full decode restores the original JSON.
        assert_eq!(fs_decode(&encoded), original);
    }

    /// `fs_fields_encode`/`fs_fields_decode` operate on the document `fields`
    /// map (not wrapped in mapValue) — pin that they're inverses.
    #[test]
    fn fs_fields_encode_decode_are_inverses() {
        let data = json!({ "a": 1, "b": "two", "c": [true, null] });
        let fields = fs_fields_encode(&data);
        assert_eq!(fields["a"], json!({"integerValue": "1"}));
        assert_eq!(fs_fields_decode(&fields), data);
    }

    /// Env vars are process-global; serialize the resolve_project cases so
    /// parallel `cargo test` threads can't race on GOOGLE_CLOUD_PROJECT /
    /// GCLOUD_PROJECT. Single shared lock held across save → mutate →
    /// assert → restore.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_env<F: FnOnce()>(keys: &[&'static str], f: F) {
        // Lock guard would dangle on panic; we want to restore env even
        // when an assertion fires, so capture state, run, restore in a
        // catch_unwind block.
        let _lock = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        let saved: Vec<_> = keys.iter().map(|k| (*k, std::env::var(k).ok())).collect();
        for k in keys {
            std::env::remove_var(k);
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        for (k, v) in &saved {
            match v {
                Some(s) => std::env::set_var(k, s),
                None => std::env::remove_var(k),
            }
        }
        if let Err(p) = result {
            std::panic::resume_unwind(p);
        }
    }

    const KEYS: &[&str] = &["GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT"];

    #[test]
    fn resolve_project_opts_wins_over_env() {
        with_env(KEYS, || {
            std::env::set_var("GOOGLE_CLOUD_PROJECT", "from-env");
            let p = resolve_project(&json!({"project": "from-opts"}));
            assert_eq!(p.as_deref(), Some("from-opts"));
        });
    }

    #[test]
    fn resolve_project_falls_back_to_google_cloud_project() {
        with_env(KEYS, || {
            std::env::set_var("GOOGLE_CLOUD_PROJECT", "primary");
            let p = resolve_project(&json!({}));
            assert_eq!(p.as_deref(), Some("primary"));
        });
    }

    #[test]
    fn resolve_project_falls_back_to_gcloud_project() {
        with_env(KEYS, || {
            std::env::set_var("GCLOUD_PROJECT", "secondary");
            let p = resolve_project(&json!({}));
            assert_eq!(p.as_deref(), Some("secondary"));
        });
    }

    #[test]
    fn resolve_project_prefers_google_cloud_project_over_gcloud_project() {
        with_env(KEYS, || {
            std::env::set_var("GOOGLE_CLOUD_PROJECT", "primary");
            std::env::set_var("GCLOUD_PROJECT", "secondary");
            let p = resolve_project(&json!({}));
            assert_eq!(p.as_deref(), Some("primary"));
        });
    }

    #[test]
    fn resolve_project_none_when_unset() {
        with_env(KEYS, || {
            let p = resolve_project(&json!({}));
            assert_eq!(p, None);
        });
    }

    #[test]
    fn resolve_project_ignores_non_string_opts() {
        with_env(KEYS, || {
            // `{"project": 42}` must not crash or stringify the integer —
            // only `as_str` survives the chain.
            let p = resolve_project(&json!({"project": 42}));
            assert_eq!(p, None);
        });
    }

    /// Empty-string project from any source is still `Some("")` — we
    /// don't filter empties out, because that would mask a real
    /// misconfiguration (env set to literal empty). Pin the behavior so
    /// future refactors that "helpfully" coerce "" -> None get caught.
    #[test]
    fn resolve_project_empty_string_in_opts_round_trips() {
        with_env(KEYS, || {
            let p = resolve_project(&json!({"project": ""}));
            assert_eq!(p.as_deref(), Some(""));
        });
    }

    /// Both env vars set but opts win — pins the precedence order so a
    /// future refactor that swaps the `or_else` chain (env-first instead
    /// of opts-first) gets caught.
    #[test]
    fn resolve_project_opts_win_over_both_env_vars() {
        with_env(KEYS, || {
            std::env::set_var("GOOGLE_CLOUD_PROJECT", "from-google");
            std::env::set_var("GCLOUD_PROJECT", "from-gcloud");
            let p = resolve_project(&json!({"project": "from-opts"}));
            assert_eq!(p.as_deref(), Some("from-opts"));
        });
    }

    #[test]
    fn resolve_project_null_opt_falls_back_to_env_not_literal_null() {
        // Catches a future regression where `as_str()` is replaced with a
        // permissive `to_string()` to "accept any type" — that would stringify
        // serde's null token to the literal `"null"`, producing a URL like
        // `?project=null` that would 400 against GCS / Pub/Sub instead of
        // falling through to GOOGLE_CLOUD_PROJECT. Pins both the null-rejection
        // invariant AND that the env fallback still fires in the same call.
        with_env(KEYS, || {
            std::env::set_var("GOOGLE_CLOUD_PROJECT", "from-env");
            let p = resolve_project(&json!({"project": null}));
            assert_eq!(p.as_deref(), Some("from-env"));
            assert_ne!(p.as_deref(), Some("null"));
        });
    }

    /// `gcp__pkg_version(NULL)` must not segfault even though the FFI
    /// caller is allowed to pass a null `args` pointer.
    ///
    /// The implementation branches on `args.is_null()` (line ~394) to
    /// substitute `Value::Null` instead of dereferencing. Any future
    /// refactor that drops the null check — e.g. switching to a single
    /// `CStr::from_ptr` call with no guard — would dereference a null
    /// pointer in release builds and abort. Pins the explicit null
    /// branch + verifies the returned pointer is a valid, NUL-terminated,
    /// JSON-parseable string carrying the expected version field.
    /// Also exercises `stryke_free_cstring` on the returned pointer to
    /// catch double-free / wrong-allocator regressions if the export ever
    /// switches away from `CString::into_raw`.
    #[test]
    fn gcp_pkg_version_null_args_returns_valid_version_json() {
        let p = gcp__pkg_version(std::ptr::null());
        assert!(!p.is_null(), "gcp__pkg_version returned null for NULL args");
        // SAFETY: p is non-null and originated from CString::into_raw above.
        let parsed: Value = unsafe {
            let cs = CStr::from_ptr(p);
            serde_json::from_slice(cs.to_bytes()).expect("returned bytes are valid JSON")
        };
        assert_eq!(
            parsed["version"].as_str(),
            Some(env!("CARGO_PKG_VERSION")),
            "version field must match CARGO_PKG_VERSION"
        );
        // Must be exactly two keys nothing: error path is mutually exclusive
        // with success path — if both appear, ffi_call_async lost track of
        // the Result/error distinction.
        assert!(
            parsed.get("error").is_none(),
            "success response must not carry an error key"
        );
        // SAFETY: p was returned by an export from this cdylib (line above).
        unsafe { stryke_free_cstring(p as *mut c_char) };
    }

    /// `gcp__pkg_version` with non-JSON garbage bytes as `args` must still
    /// succeed because the JSON parse error is intentionally swallowed in
    /// `ffi_call_async` (`unwrap_or(Value::Null)`, line ~398) and the
    /// `pkg_version` handler ignores its input.
    ///
    /// Catches a future regression where someone replaces the
    /// `unwrap_or(Value::Null)` with `unwrap()` to "surface parse errors" —
    /// that would convert every malformed-args call from a successful
    /// version probe into a panic-caught `{"error": "stryke-gcp handler
    /// panicked"}`, breaking stryke's version sniff on hosts where the
    /// FFI bridge sends an empty or non-JSON args block. Distinct from
    /// the NULL test above — the NULL path takes the `is_null()` branch;
    /// this path exercises the SECOND branch (CStr parse → JSON fail →
    /// fallback to Null).
    #[test]
    fn gcp_pkg_version_garbage_args_does_not_panic_or_error() {
        let garbage = CString::new("not json at all { ] }").unwrap();
        let p = gcp__pkg_version(garbage.as_ptr());
        assert!(!p.is_null());
        let parsed: Value = unsafe {
            let cs = CStr::from_ptr(p);
            serde_json::from_slice(cs.to_bytes()).expect("output JSON parses")
        };
        assert_eq!(parsed["version"].as_str(), Some(env!("CARGO_PKG_VERSION")));
        assert!(
            parsed.get("error").is_none(),
            "garbage args must not produce an error response — \
             pkg_version handler ignores its input"
        );
        unsafe { stryke_free_cstring(p as *mut c_char) };
    }

    /// `stryke_free_cstring(NULL)` must be a documented no-op.
    ///
    /// The FFI contract (docstring above the export) promises null is
    /// safe. The current impl has an explicit `if p.is_null() { return; }`
    /// guard. Catches a future regression where the guard is removed and
    /// `CString::from_raw(null)` runs — that is UB and on most allocators
    /// aborts the test process. Running the no-op a few times also catches
    /// a bug where someone "optimizes" the guard into a `debug_assert!`
    /// that would let release callers segfault while tests pass.
    #[test]
    fn stryke_free_cstring_null_is_noop() {
        unsafe {
            stryke_free_cstring(std::ptr::null_mut());
            stryke_free_cstring(std::ptr::null_mut());
            stryke_free_cstring(std::ptr::null_mut());
        }
    }

    #[test]
    fn resolve_project_rejects_array_and_object_opts() {
        // Distinct from the integer case: arrays/objects are also Value
        // variants whose `Display` impl produces `"[]"` / `"{}"` — any future
        // refactor that switches from `as_str()` to `to_string()` /
        // `serde_json::to_string()` would silently emit those bracket strings
        // as the project ID and corrupt every REST URL. The integer test
        // alone does NOT catch this because `42.to_string()` looks plausible
        // ("42" is a valid project-name char set); brackets are the unambiguous
        // tell.
        with_env(KEYS, || {
            std::env::set_var("GOOGLE_CLOUD_PROJECT", "real-project");
            let p_arr = resolve_project(&json!({"project": []}));
            assert_eq!(p_arr.as_deref(), Some("real-project"));
            assert_ne!(p_arr.as_deref(), Some("[]"));

            let p_obj = resolve_project(&json!({"project": {}}));
            assert_eq!(p_obj.as_deref(), Some("real-project"));
            assert_ne!(p_obj.as_deref(), Some("{}"));
        });
    }

    /// FFI error-path round-trip: a handler that REQUIRES a field (e.g.
    /// `op_gcs_list_objects` needs `bucket`) must, when called with a NULL
    /// args pointer, return a non-null, NUL-terminated, JSON-parseable
    /// CString carrying `{"error": "missing bucket"}` — NOT a null pointer,
    /// NOT a panic, NOT a partial response. The handler's missing-field check
    /// (`opts["bucket"].as_str().ok_or_else(...)`) runs BEFORE auth_header(),
    /// so this is fully deterministic in CI without ADC.
    ///
    /// What this catches that existing tests do NOT:
    /// 1. `gcp_pkg_version_null_args_returns_valid_version_json` covers the
    ///    SUCCESS path of ffi_call_async (Ok(Value) branch). This covers the
    ///    Err(anyhow!(...)) branch — a separate arm in the match.
    /// 2. A future refactor that returns `std::ptr::null()` on handler error
    ///    instead of a CString would crash any C caller that calls
    ///    `stryke_free_cstring(p)` without a null check (or worse, calls
    ///    `parse_json(p)` expecting a JSON pointer per the export contract).
    /// 3. A future refactor that drops the `catch_unwind` and lets a
    ///    handler-side `anyhow!` macro internal panic propagate would also
    ///    fail this test (process abort during cargo test).
    /// 4. Verifies the returned pointer survives `stryke_free_cstring` — if
    ///    the error path ever allocates via a different allocator (e.g.
    ///    constructing the JSON via a custom arena) than `CString::into_raw`
    ///    uses, the free would be a wrong-allocator UB.
    #[test]
    fn ffi_error_path_missing_field_returns_freeable_json_cstring() {
        let p = gcp__gcs_list_objects(std::ptr::null());
        assert!(
            !p.is_null(),
            "FFI error path must return a CString, not a raw null"
        );
        // SAFETY: p originated from CString::into_raw in ffi_call_async.
        let parsed: Value = unsafe {
            let cs = CStr::from_ptr(p);
            serde_json::from_slice(cs.to_bytes()).expect("error response must be valid JSON")
        };
        assert_eq!(
            parsed["error"].as_str(),
            Some("missing bucket"),
            "error message must match the handler's anyhow! literal"
        );
        // The success-shape keys must NOT appear alongside `error` —
        // catches a regression where ffi_call_async merges both branches.
        assert!(
            parsed.get("bucket").is_none(),
            "error response must not carry success-shape `bucket` key"
        );
        assert!(
            parsed.get("objects").is_none(),
            "error response must not carry success-shape `objects` key"
        );
        // SAFETY: p was returned by this cdylib's export (line above).
        unsafe { stryke_free_cstring(p as *mut c_char) };
    }

    /// FFI error path with GARBAGE (non-JSON) args bytes: the
    /// `serde_json::from_slice(cs.to_bytes()).unwrap_or(Value::Null)` fallback
    /// in `ffi_call_async` (line ~398) must convert parse failure into
    /// `Value::Null`, which then makes `opts["bucket"].as_str()` return None,
    /// which the handler converts to a clean `{"error": "missing bucket"}`.
    ///
    /// Distinct from `ffi_error_path_missing_field_returns_freeable_json_cstring`
    /// above — that test exercises the `args.is_null()` BRANCH (first arm of
    /// the if). This one exercises the SECOND arm: non-null pointer, JSON
    /// parse fails, fallback Value::Null, handler errors cleanly. Both
    /// paths must converge on the same observable behavior (error JSON +
    /// freeable pointer), otherwise the cdylib's contract is asymmetric
    /// across NULL vs garbage input — which would surface as a hard-to-debug
    /// stryke-side bug where some hosts work and others don't depending on
    /// whether the FFI bridge sends NULL or empty-string args.
    ///
    /// Catches: a future refactor that replaces `unwrap_or(Value::Null)` with
    /// `unwrap()` would panic here → caught by `catch_unwind` → return
    /// `{"error":"stryke-gcp handler panicked"}`, masking the real "missing
    /// bucket" diagnostic the user needs to fix their call.
    #[test]
    fn ffi_garbage_args_to_field_requiring_handler_yields_missing_field_error() {
        let garbage = CString::new("{not valid json at all }").unwrap();
        let p = gcp__gcs_list_objects(garbage.as_ptr());
        assert!(!p.is_null());
        let parsed: Value = unsafe {
            let cs = CStr::from_ptr(p);
            serde_json::from_slice(cs.to_bytes()).expect("output JSON parses")
        };
        // The CRUCIAL invariant: we must see "missing bucket" — NOT
        // "stryke-gcp handler panicked". A panic-caught error here would
        // prove ffi_call_async lost the Value::Null fallback.
        assert_eq!(
            parsed["error"].as_str(),
            Some("missing bucket"),
            "garbage args must fall back to Value::Null and surface the \
             handler's own missing-field error, not a panic-caught generic"
        );
        assert_ne!(
            parsed["error"].as_str(),
            Some("stryke-gcp handler panicked"),
            "ffi_call_async must NOT lose the Value::Null fallback on parse fail"
        );
        unsafe { stryke_free_cstring(p as *mut c_char) };
    }

    /// `op_pubsub_ack` builds the request body from
    /// `ack_ids.iter().filter_map(|v| v.as_str().map(String::from))` and
    /// then reports `acked: ack_ids.len()` — the FILTERED length, not the
    /// input length. If the caller passes a mixed array `["good", 42, null,
    /// "also-good"]`, only the two strings make it into the actual
    /// `ackIds` payload sent to Pub/Sub, and only those two are counted.
    ///
    /// This pins the silent-drop semantics for non-string array elements.
    /// The bug class it catches: a future refactor that "improves" the
    /// reported count by recording `opts["ack_ids"].as_array().unwrap().len()`
    /// at the top of the function (before the filter) would lie to the
    /// caller about how many messages were actually acked — they'd think
    /// 4 succeeded when only 2 were submitted, causing the other 2 to
    /// redeliver indefinitely.
    ///
    /// We can't test the HTTP path without a fake server, but we CAN
    /// observe the filter behavior via the FFI error path: pass an
    /// `ack_ids` array of only non-strings, the filter produces an empty
    /// Vec, but the field IS present (so the `as_array().ok_or_else` check
    /// passes), and the request fails at auth (no ADC in CI). The error
    /// message therefore tells us whether the function got PAST the
    /// ack_ids parsing. If a regression makes ack_ids parsing reject
    /// non-string-only arrays earlier (e.g. someone adds an
    /// `if ack_ids.is_empty()` guard with a different error literal), this
    /// test will see a different error string.
    ///
    /// What this is NOT: a smoke test. It pins a specific invariant
    /// (parse → filter → no early reject for empty filtered Vec → reach
    /// auth) that is structurally distinct from any test in the file.
    #[test]
    fn pubsub_ack_filter_admits_empty_filtered_vec_past_parse() {
        with_env(KEYS, || {
            std::env::set_var("GOOGLE_CLOUD_PROJECT", "test-project");
            // ack_ids array is present (so as_array succeeds) but contains
            // only non-strings (so the filter_map yields empty).
            let args = CString::new(
                serde_json::to_string(&json!({
                    "subscription": "test-sub",
                    "ack_ids": [42, null, true, {"foo": "bar"}],
                }))
                .unwrap(),
            )
            .unwrap();
            let p = gcp__pubsub_ack(args.as_ptr());
            assert!(!p.is_null());
            let parsed: Value = unsafe {
                let cs = CStr::from_ptr(p);
                serde_json::from_slice(cs.to_bytes()).expect("output JSON parses")
            };
            // Whatever the error is, it MUST NOT be "missing ack_ids
            // (array)" — that would mean a regression added an emptiness
            // check that incorrectly rejected the array.
            let err = parsed["error"].as_str().unwrap_or("");
            assert_ne!(
                err, "missing ack_ids (array)",
                "non-string ack_ids array must still pass the as_array() check; \
                 got error: {err}"
            );
            // Also must not be the project-missing literal — project IS
            // resolved from env. Catches a regression where env resolution
            // order changes silently.
            assert_ne!(
                err, "missing project",
                "GOOGLE_CLOUD_PROJECT was set; resolve_project must succeed"
            );
            // Also must not be subscription-missing — we passed it.
            assert_ne!(
                err, "missing subscription",
                "subscription was provided in args"
            );
            unsafe { stryke_free_cstring(p as *mut c_char) };
        });
    }

    /// `op_gcs_get_object` validates `bucket` (line ~176) BEFORE `key`
    /// (line ~179). When BOTH are absent the caller must see "missing
    /// bucket" — the FIRST check — and never reach the `key` check. When
    /// `bucket` is present but `key` is absent the caller must see
    /// "missing key". This pins the validation ORDER.
    ///
    /// Why it matters / what it catches: a future refactor that hoists the
    /// `key` extraction above the `bucket` extraction (e.g. to build the
    /// object path first) would flip the reported diagnostic — a caller who
    /// forgot BOTH would be told "missing key" while still also lacking a
    /// bucket, sending them to fix the wrong argument. Distinct from the
    /// existing `op_gcs_list_objects` "missing bucket" test: that handler
    /// has no `key` field at all, so it cannot exercise the two-field
    /// precedence chain. Both checks run before `auth_header()`, so this is
    /// deterministic in CI with no ADC.
    #[test]
    fn gcs_get_object_validates_bucket_before_key() {
        // Both missing → first check fires → "missing bucket".
        let p_both = gcp__gcs_get_object(std::ptr::null());
        assert!(!p_both.is_null());
        let parsed_both: Value = unsafe {
            let cs = CStr::from_ptr(p_both);
            serde_json::from_slice(cs.to_bytes()).expect("error JSON parses")
        };
        assert_eq!(
            parsed_both["error"].as_str(),
            Some("missing bucket"),
            "bucket is checked first; both-missing must report bucket, not key"
        );
        unsafe { stryke_free_cstring(p_both as *mut c_char) };

        // bucket present, key absent → second check fires → "missing key".
        let args = CString::new(serde_json::to_string(&json!({"bucket": "b"})).unwrap()).unwrap();
        let p_key = gcp__gcs_get_object(args.as_ptr());
        assert!(!p_key.is_null());
        let parsed_key: Value = unsafe {
            let cs = CStr::from_ptr(p_key);
            serde_json::from_slice(cs.to_bytes()).expect("error JSON parses")
        };
        assert_eq!(
            parsed_key["error"].as_str(),
            Some("missing key"),
            "bucket supplied; only key is missing — must report key, not bucket"
        );
        unsafe { stryke_free_cstring(p_key as *mut c_char) };
    }

    /// `op_pubsub_publish` resolves `project` (line ~271) BEFORE validating
    /// `topic` (line ~272). With `GOOGLE_CLOUD_PROJECT` set but `topic`
    /// absent, the caller must reach the topic check and see "missing
    /// topic". With NO project source AND topic absent, the project check
    /// fires FIRST and the caller must see "missing project" — never
    /// "missing topic".
    ///
    /// Bug class caught: a future refactor that validates `topic` before
    /// resolving `project` would tell a caller who is missing BOTH that
    /// `topic` is the problem, masking the more fundamental missing-project
    /// misconfiguration (which also can't be fixed by adding a topic). This
    /// pins the resolve-project-first precedence specifically for the
    /// publish path, which is structurally distinct from the ack path tested
    /// in `pubsub_ack_filter_admits_empty_filtered_vec_past_parse` (that test
    /// always has project set and never exercises the project-missing arm).
    /// Both checks precede `auth_header()`, so deterministic without ADC.
    #[test]
    fn pubsub_publish_resolves_project_before_topic() {
        // Project resolvable from env, topic absent → reach topic check.
        with_env(KEYS, || {
            std::env::set_var("GOOGLE_CLOUD_PROJECT", "test-project");
            let args = CString::new(serde_json::to_string(&json!({"data": "x"})).unwrap()).unwrap();
            let p = gcp__pubsub_publish(args.as_ptr());
            assert!(!p.is_null());
            let parsed: Value = unsafe {
                let cs = CStr::from_ptr(p);
                serde_json::from_slice(cs.to_bytes()).expect("error JSON parses")
            };
            assert_eq!(
                parsed["error"].as_str(),
                Some("missing topic"),
                "project resolved from env; topic absent — must report topic"
            );
            unsafe { stryke_free_cstring(p as *mut c_char) };
        });

        // No project source at all, topic also absent → project check first.
        with_env(KEYS, || {
            let args = CString::new(serde_json::to_string(&json!({"data": "x"})).unwrap()).unwrap();
            let p = gcp__pubsub_publish(args.as_ptr());
            assert!(!p.is_null());
            let parsed: Value = unsafe {
                let cs = CStr::from_ptr(p);
                serde_json::from_slice(cs.to_bytes()).expect("error JSON parses")
            };
            assert_eq!(
                parsed["error"].as_str(),
                Some("missing project"),
                "no project source; project check fires before topic check"
            );
            assert_ne!(
                parsed["error"].as_str(),
                Some("missing topic"),
                "must not report topic when project is the first unmet requirement"
            );
            unsafe { stryke_free_cstring(p as *mut c_char) };
        });
    }

    /// Pub/Sub list endpoints return fully-qualified resource paths
    /// (`projects/p/topics/t`); `leaf_name` must strip them to the bare name
    /// so callers get `t`, not the whole path.
    #[test]
    fn leaf_name_strips_resource_path() {
        assert_eq!(leaf_name("projects/my-proj/topics/orders"), "orders");
        assert_eq!(leaf_name("projects/my-proj/subscriptions/sub-1"), "sub-1");
        assert_eq!(leaf_name("orders"), "orders");
        assert_eq!(leaf_name(""), "");
    }

    // ── pure helpers (no GCP) ────────────────────────────────────────────────

    #[test]
    fn parse_gs_uri_splits_bucket_and_object() {
        let v = op_parse_gs_uri(json!({"uri": "gs://my-bucket/path/to/file.json"})).unwrap();
        assert_eq!(v["bucket"], json!("my-bucket"));
        assert_eq!(v["object"], json!("path/to/file.json"));
        let bucket_only = op_parse_gs_uri(json!({"uri": "gs://my-bucket"})).unwrap();
        assert_eq!(bucket_only["object"], Value::Null);
        assert!(op_parse_gs_uri(json!({"uri": "s3://x/y"})).is_err());
    }

    #[test]
    fn build_gs_uri_round_trips_through_parse() {
        let built = op_build_gs_uri(json!({"bucket": "my-bucket", "object": "path/to/file.json"}))
            .unwrap()["uri"]
            .clone();
        assert_eq!(built, json!("gs://my-bucket/path/to/file.json"));
        let back = op_parse_gs_uri(json!({"uri": built})).unwrap();
        assert_eq!(back["bucket"], json!("my-bucket"));
        assert_eq!(back["object"], json!("path/to/file.json"));
        // Bare bucket when object omitted or empty; leading slash trimmed.
        assert_eq!(
            op_build_gs_uri(json!({"bucket": "b"})).unwrap()["uri"],
            json!("gs://b")
        );
        assert_eq!(
            op_build_gs_uri(json!({"bucket": "b", "object": ""})).unwrap()["uri"],
            json!("gs://b")
        );
        assert_eq!(
            op_build_gs_uri(json!({"bucket": "b", "object": "/leading"})).unwrap()["uri"],
            json!("gs://b/leading")
        );
        assert!(op_build_gs_uri(json!({"bucket": ""})).is_err());
    }

    #[test]
    fn gs_uri_to_url_maps_to_path_style_public_url() {
        let obj = op_gs_uri_to_url(json!({"uri": "gs://my-bucket/path/to/file.json"})).unwrap();
        assert_eq!(
            obj["url"],
            json!("https://storage.googleapis.com/my-bucket/path/to/file.json")
        );
        assert_eq!(obj["bucket"], json!("my-bucket"));
        assert_eq!(obj["object"], json!("path/to/file.json"));
        // Bucket-only URI → bucket endpoint, null object.
        let buck = op_gs_uri_to_url(json!({"uri": "gs://my-bucket"})).unwrap();
        assert_eq!(
            buck["url"],
            json!("https://storage.googleapis.com/my-bucket")
        );
        assert_eq!(buck["object"], Value::Null);
        // Trailing slash with no object is still the bucket endpoint.
        assert_eq!(
            op_gs_uri_to_url(json!({"uri": "gs://b/"})).unwrap()["url"],
            json!("https://storage.googleapis.com/b")
        );
        assert!(op_gs_uri_to_url(json!({"uri": "s3://x/y"})).is_err());
    }

    #[test]
    fn url_to_gs_uri_inverts_gs_uri_to_url() {
        // Path-style URL → object URI.
        let obj = op_url_to_gs_uri(
            json!({"url": "https://storage.googleapis.com/my-bucket/path/to/file.json"}),
        )
        .unwrap();
        assert_eq!(obj["uri"], json!("gs://my-bucket/path/to/file.json"));
        assert_eq!(obj["bucket"], json!("my-bucket"));
        assert_eq!(obj["object"], json!("path/to/file.json"));
        // Virtual-hosted-style URL → same URI.
        let vh =
            op_url_to_gs_uri(json!({"url": "https://my-bucket.storage.googleapis.com/a/b.txt"}))
                .unwrap();
        assert_eq!(vh["uri"], json!("gs://my-bucket/a/b.txt"));
        assert_eq!(vh["bucket"], json!("my-bucket"));
        // commondatastorage alias, bucket-only.
        assert_eq!(
            op_url_to_gs_uri(json!({"url": "https://commondatastorage.googleapis.com/b"})).unwrap()
                ["uri"],
            json!("gs://b")
        );
        // Round-trips with gs_uri_to_url for path and bucket-only forms.
        for uri in ["gs://b/k", "gs://b", "gs://my-bucket/deep/key.bin"] {
            let url = op_gs_uri_to_url(json!({"uri": uri})).unwrap()["url"]
                .as_str()
                .unwrap()
                .to_string();
            assert_eq!(
                op_url_to_gs_uri(json!({"url": url})).unwrap()["uri"],
                json!(uri)
            );
        }
        // Non-GCS host and non-http scheme reject.
        assert!(op_url_to_gs_uri(json!({"url": "https://example.com/b/k"})).is_err());
        assert!(op_url_to_gs_uri(json!({"url": "gs://b/k"})).is_err());
    }

    #[test]
    fn parse_resource_name_pairs_collections_with_ids() {
        let v = op_parse_resource_name(json!({
            "name": "projects/my-proj/topics/orders"
        }))
        .unwrap();
        assert_eq!(v["pairs"]["projects"], json!("my-proj"));
        assert_eq!(v["pairs"]["topics"], json!("orders"));
        assert_eq!(
            v["trailing"],
            Value::Null,
            "even path has no trailing segment"
        );
        // Odd path (a collection with no id) surfaces a trailing segment.
        let odd = op_parse_resource_name(json!({"name": "projects/p/secrets"})).unwrap();
        assert_eq!(odd["pairs"]["projects"], json!("p"));
        assert_eq!(odd["trailing"], json!("secrets"));
    }

    #[test]
    fn resource_name_parent_drops_the_leaf_pair() {
        // A nested resource: parent strips the trailing collection/id pair.
        let v = op_resource_name_parent(json!({
            "name": "projects/p/locations/l/clusters/c"
        }))
        .unwrap();
        assert_eq!(v["parent"], json!("projects/p/locations/l"));
        assert_eq!(v["collection"], json!("clusters"));
        assert_eq!(v["id"], json!("c"));
        assert_eq!(v["has_parent"], json!(true));
        // A top-level resource has an empty parent.
        let top = op_resource_name_parent(json!({"name": "projects/my-proj"})).unwrap();
        assert_eq!(top["parent"], json!(""));
        assert_eq!(top["has_parent"], json!(false));
        assert_eq!(top["collection"], json!("projects"));
        assert_eq!(top["id"], json!("my-proj"));
        // Leading/trailing slashes are tolerated; `resource` is an alias.
        assert_eq!(
            op_resource_name_parent(json!({"resource": "/projects/p/topics/t/"})).unwrap()
                ["parent"],
            json!("projects/p")
        );
        // A dangling collection (odd segment count) and missing arg error.
        assert!(op_resource_name_parent(json!({"name": "projects/p/secrets"})).is_err());
        assert!(op_resource_name_parent(json!({"name": ""})).is_err());
        assert!(op_resource_name_parent(json!({})).is_err());
    }

    #[test]
    fn build_resource_name_inverts_parse_resource_name() {
        // From a flat parts list — the exact inverse.
        assert_eq!(
            op_build_resource_name(json!({"parts": ["projects", "p", "topics", "t"]})).unwrap()
                ["name"],
            json!("projects/p/topics/t")
        );
        // From ordered pairs (arrays) plus a trailing segment.
        assert_eq!(
            op_build_resource_name(json!({
                "pairs": [["projects", "p"], ["secrets", "s"]],
                "trailing": "versions"
            }))
            .unwrap()["name"],
            json!("projects/p/secrets/s/versions")
        );
        // Pairs as objects work too.
        assert_eq!(
            op_build_resource_name(json!({"pairs": [{"collection": "projects", "id": "p"}]}))
                .unwrap()["name"],
            json!("projects/p")
        );
        // Round-trips the parsed `parts` of any resource name.
        for name in ["projects/p/topics/t", "projects/p/secrets", "buckets/b"] {
            let parsed = op_parse_resource_name(json!({ "name": name })).unwrap();
            let rebuilt = op_build_resource_name(json!({ "parts": parsed["parts"] })).unwrap()
                ["name"]
                .clone();
            assert_eq!(rebuilt, json!(name), "round-trip for {name}");
        }
        // Empty parts, a `/`-bearing segment, and no input all error.
        assert!(op_build_resource_name(json!({"parts": []})).is_err());
        assert!(op_build_resource_name(json!({"parts": ["a/b", "c"]})).is_err());
        assert!(op_build_resource_name(json!({})).is_err());
    }

    #[test]
    fn parse_table_ref_handles_standard_and_legacy_forms() {
        // Standard SQL: dots throughout.
        let std = op_parse_table_ref(json!({"table_ref": "my-proj.sales.orders"})).unwrap();
        assert_eq!(std["project"], json!("my-proj"));
        assert_eq!(std["dataset"], json!("sales"));
        assert_eq!(std["table"], json!("orders"));
        assert_eq!(std["legacy"], json!(false));
        // Legacy SQL / bq CLI: a `:` after the project.
        let leg = op_parse_table_ref(json!({"table_ref": "my-proj:sales.orders"})).unwrap();
        assert_eq!(leg["project"], json!("my-proj"));
        assert_eq!(leg["dataset"], json!("sales"));
        assert_eq!(leg["table"], json!("orders"));
        assert_eq!(leg["legacy"], json!(true));
        // Project-less dataset.table → project is null.
        let bare = op_parse_table_ref(json!({"table_ref": "sales.orders"})).unwrap();
        assert_eq!(bare["project"], Value::Null);
        assert_eq!(bare["dataset"], json!("sales"));
        assert_eq!(bare["table"], json!("orders"));
        // `value` alias.
        assert_eq!(
            op_parse_table_ref(json!({"value": "p.d.t"})).unwrap()["table"],
            json!("t")
        );
        // Errors: bare table, empty segment, two colons, missing.
        assert!(op_parse_table_ref(json!({"table_ref": "orders"})).is_err());
        assert!(op_parse_table_ref(json!({"table_ref": "proj..table"})).is_err());
        assert!(op_parse_table_ref(json!({"table_ref": "a:b:c.d"})).is_err());
        assert!(op_parse_table_ref(json!({"table_ref": "p.d.t.extra"})).is_err());
        assert!(op_parse_table_ref(json!({})).is_err());
    }

    #[test]
    fn build_table_ref_inverts_parse_table_ref() {
        // Standard dotted form with a project.
        assert_eq!(
            op_build_table_ref(json!({"project": "p", "dataset": "d", "table": "t"})).unwrap()
                ["table_ref"],
            json!("p.d.t")
        );
        // Legacy colon form.
        assert_eq!(
            op_build_table_ref(
                json!({"project": "p", "dataset": "d", "table": "t", "legacy": true})
            )
            .unwrap()["table_ref"],
            json!("p:d.t")
        );
        // Project-less → dataset.table.
        assert_eq!(
            op_build_table_ref(json!({"dataset": "d", "table": "t"})).unwrap()["table_ref"],
            json!("d.t")
        );
        // Round-trips parse_table_ref both ways (project field is null vs string).
        for av in [
            "my-proj.sales.orders",
            "my-proj:sales.orders",
            "sales.orders",
        ] {
            let p = op_parse_table_ref(json!({ "table_ref": av })).unwrap();
            let rebuilt = op_build_table_ref(json!({
                "project": p["project"], "dataset": p["dataset"],
                "table": p["table"], "legacy": p["legacy"],
            }))
            .unwrap();
            assert_eq!(rebuilt["table_ref"], json!(av), "round-trip {av}");
        }
        // Errors: missing dataset/table, a separator in a segment, legacy w/o project.
        assert!(op_build_table_ref(json!({"table": "t"})).is_err());
        assert!(op_build_table_ref(json!({"dataset": "d"})).is_err());
        assert!(op_build_table_ref(json!({"dataset": "a.b", "table": "t"})).is_err());
        assert!(op_build_table_ref(json!({"dataset": "d", "table": "t", "legacy": true})).is_err());
        assert!(op_build_table_ref(json!({})).is_err());
    }

    #[test]
    fn valid_bucket_name_enforces_gcs_rules() {
        // Underscores are legal in GCS (unlike S3).
        assert_eq!(
            op_valid_bucket_name(json!({"name": "my_data.bucket-1"})).unwrap()["valid"],
            json!(true)
        );
        for (name, want) in [
            ("ab", "3-63"),
            ("UPPER", "lowercase"),
            ("-bad", "start and end"),
            ("10.0.0.1", "IP address"),
            ("goog-private", "goog"),
            ("my-google-data", "google"),
        ] {
            let v = op_valid_bucket_name(json!({"name": name})).unwrap();
            assert_eq!(v["valid"], json!(false), "{name} should be invalid");
            assert!(
                v["reason"].as_str().unwrap().contains(want),
                "{name}: reason `{}` should mention `{want}`",
                v["reason"]
            );
        }
    }

    #[test]
    fn valid_project_id_enforces_gcp_rules() {
        let ok = |id: &str| {
            op_valid_project_id(json!({ "id": id })).unwrap()["valid"]
                .as_bool()
                .unwrap()
        };
        assert!(ok("my-project-123"));
        assert!(ok("abcdef"), "exactly 6 chars");
        assert!(ok(&format!("a{}", "b".repeat(29))), "exactly 30 chars");
        for (id, want) in [
            ("short", "6-30"),
            ("1starts-with-digit", "lowercase letter"),
            ("Uppercase-id", "lowercase letter"),
            ("ends-with-dash-", "end with a hyphen"),
            ("has_underscore", "hyphens"),
        ] {
            let v = op_valid_project_id(json!({ "id": id })).unwrap();
            assert_eq!(v["valid"], json!(false), "{id} should be invalid");
            assert!(
                v["reason"].as_str().unwrap().contains(want),
                "{id}: reason `{}` should mention `{want}`",
                v["reason"]
            );
        }
        assert!(!ok(&"a".repeat(31)), "31 chars exceeds the max");
        assert!(op_valid_project_id(json!({})).is_err());
    }

    #[test]
    fn valid_object_name_enforces_gcs_hard_rules() {
        let chk = |n: &str| op_valid_object_name(json!({ "name": n })).unwrap();
        // Ordinary keys with slashes, dots, unicode all pass.
        for ok in [
            "a",
            "dir/sub/file.txt",
            "a.b.c",
            "naïve.txt",
            "with spaces.png",
            &"x".repeat(1024),
        ] {
            assert_eq!(chk(ok)["valid"], json!(true), "`{ok}` should be valid");
        }
        // Empty and over-1024-bytes fail.
        assert_eq!(chk("")["valid"], json!(false));
        assert_eq!(chk(&"x".repeat(1025))["valid"], json!(false));
        // A multibyte char counts its UTF-8 bytes toward the 1024 limit.
        assert_eq!(chk(&"é".repeat(513))["valid"], json!(false)); // 513*2 = 1026 bytes
                                                                  // CR / LF forbidden.
        assert_eq!(chk("a\rb")["valid"], json!(false));
        assert_eq!(chk("a\nb")["valid"], json!(false));
        // `.` and `..` forbidden; but `...` and `.foo` are fine.
        assert_eq!(chk(".")["valid"], json!(false));
        assert_eq!(chk("..")["valid"], json!(false));
        assert_eq!(chk("...")["valid"], json!(true));
        assert_eq!(chk(".hidden")["valid"], json!(true));
        // The ACME challenge prefix is forbidden.
        assert_eq!(
            chk(".well-known/acme-challenge/token")["valid"],
            json!(false)
        );
        // A different .well-known path is fine.
        assert_eq!(chk(".well-known/other")["valid"], json!(true));
        // Missing name errors.
        assert!(op_valid_object_name(json!({})).is_err());
    }

    #[test]
    fn region_for_zone_strips_the_zone_letter() {
        let v = op_region_for_zone(json!({"zone": "us-central1-a"})).unwrap();
        assert_eq!(v["region"], json!("us-central1"));
        assert_eq!(v["zone_letter"], json!("a"));
        // Multi-hyphen regions still resolve (only the last segment is stripped).
        assert_eq!(
            op_region_for_zone(json!({"zone": "europe-west4-b"})).unwrap()["region"],
            json!("europe-west4")
        );
        assert_eq!(
            op_region_for_zone(json!({"zone": "asia-northeast1-c"})).unwrap()["region"],
            json!("asia-northeast1")
        );
        // No hyphen, or a multi-char / non-letter suffix, errors.
        assert!(op_region_for_zone(json!({"zone": "uscentral1"})).is_err());
        assert!(op_region_for_zone(json!({"zone": "us-central1-ab"})).is_err());
        assert!(op_region_for_zone(json!({"zone": "us-central1-1"})).is_err());
        assert!(op_region_for_zone(json!({})).is_err());
    }

    #[test]
    fn valid_label_enforces_resource_manager_rules() {
        let chk = |k: &str, v: &str| op_valid_label(json!({"key": k, "value": v})).unwrap();
        // Valid: lowercase, digits, _ and -, value may be empty.
        assert_eq!(chk("env", "prod")["valid"], json!(true));
        assert_eq!(chk("cost-center_1", "team-a_2")["valid"], json!(true));
        assert_eq!(
            chk("team", "")["valid"],
            json!(true),
            "empty value is allowed"
        );
        // Invalids with reason fragments.
        for (k, v, want) in [
            ("", "x", "key must not be empty"),
            ("1env", "x", "start with a lowercase letter"),
            ("Env", "x", "start with a lowercase letter"),
            ("env.x", "x", "key may contain"),
            ("env", "Prod", "value may contain"),
            ("env", "a b", "value may contain"),
        ] {
            let r = op_valid_label(json!({"key": k, "value": v})).unwrap();
            assert_eq!(r["valid"], json!(false), "`{k}`=`{v}` should be invalid");
            assert!(
                r["reason"].as_str().unwrap().contains(want),
                "`{k}`=`{v}`: reason `{}` should mention `{want}`",
                r["reason"]
            );
        }
        // 63-char key is the max.
        assert_eq!(
            chk(&format!("a{}", "b".repeat(62)), "x")["valid"],
            json!(true)
        );
        assert_eq!(
            chk(&format!("a{}", "b".repeat(63)), "x")["valid"],
            json!(false)
        );
        // Missing value defaults to empty (valid).
        assert_eq!(
            op_valid_label(json!({"key": "env"})).unwrap()["valid"],
            json!(true)
        );
        assert!(op_valid_label(json!({})).is_err());
    }

    #[test]
    fn valid_pubsub_id_enforces_naming_guidelines() {
        let chk = |id: &str| op_valid_pubsub_id(json!({ "id": id })).unwrap();
        // Valid: starts with a letter, allowed punctuation, 3-255 chars.
        assert_eq!(chk("my-topic")["valid"], json!(true));
        assert_eq!(chk("orders.v2_dead-letter~1+a%20")["valid"], json!(true));
        assert_eq!(chk("abc")["valid"], json!(true), "3 chars is the min");
        // Too short / too long.
        assert_eq!(chk("ab")["valid"], json!(false));
        assert!(chk("ab")["reason"].as_str().unwrap().contains("3-255"));
        assert_eq!(chk(&"a".repeat(256))["valid"], json!(false));
        assert_eq!(chk(&"a".repeat(255))["valid"], json!(true));
        // Must start with a letter.
        let dig = chk("1topic");
        assert_eq!(dig["valid"], json!(false));
        assert!(dig["reason"]
            .as_str()
            .unwrap()
            .contains("start with a letter"));
        // The `goog` prefix is reserved.
        let g = chk("googtopic");
        assert_eq!(g["valid"], json!(false));
        assert!(g["reason"].as_str().unwrap().contains("goog"));
        // Capitalized `Goog` is NOT the reserved prefix → allowed.
        assert_eq!(chk("Googtopic")["valid"], json!(true));
        // Disallowed characters.
        for bad in ["has space", "slash/no", "dollar$", "comma,no"] {
            assert_eq!(chk(bad)["valid"], json!(false), "`{bad}` should be invalid");
        }
        // `name` is accepted as an alias for `id`.
        assert_eq!(
            op_valid_pubsub_id(json!({"name": "my-sub"})).unwrap()["valid"],
            json!(true)
        );
        assert!(op_valid_pubsub_id(json!({})).is_err());
    }

    #[test]
    fn valid_service_account_id_enforces_iam_account_id_rules() {
        let chk = |id: &str| op_valid_service_account_id(json!({ "id": id })).unwrap();
        // Valid: 6-30 chars, lowercase/digit/hyphen, starts with a letter.
        assert_eq!(chk("my-svc")["valid"], json!(true), "6 chars is the min");
        assert_eq!(chk("deploy-bot-01")["valid"], json!(true));
        assert_eq!(
            chk(&"a".repeat(30))["valid"],
            json!(true),
            "30 chars is the max"
        );
        // Length bounds.
        assert_eq!(chk("svc")["valid"], json!(false), "5 chars is too short");
        assert!(chk("abc")["reason"].as_str().unwrap().contains("6-30"));
        assert_eq!(chk(&"a".repeat(31))["valid"], json!(false));
        // Must start with a lowercase letter (not a digit or hyphen).
        let dig = chk("1service");
        assert_eq!(dig["valid"], json!(false));
        assert!(dig["reason"]
            .as_str()
            .unwrap()
            .contains("start with a lowercase letter"));
        assert_eq!(chk("-service")["valid"], json!(false));
        // Must not end with a hyphen (RFC1035).
        let trail = chk("service-");
        assert_eq!(trail["valid"], json!(false));
        assert!(trail["reason"]
            .as_str()
            .unwrap()
            .contains("end with a hyphen"));
        // Disallowed characters: uppercase, underscore, dot.
        for bad in ["MyService", "my_service", "my.service", "my svc"] {
            assert_eq!(chk(bad)["valid"], json!(false), "`{bad}` should be invalid");
        }
        // `account_id` alias and the missing-arg error.
        assert_eq!(
            op_valid_service_account_id(json!({"account_id": "billing-export"})).unwrap()["valid"],
            json!(true)
        );
        assert!(op_valid_service_account_id(json!({})).is_err());
    }

    #[test]
    fn valid_secret_id_enforces_secret_manager_rules() {
        let chk = |id: &str| op_valid_secret_id(json!({ "id": id })).unwrap();
        // Valid: mixed case, digits, hyphens, underscores; no leading/trailing rules.
        assert_eq!(chk("my-Secret_1")["valid"], json!(true));
        assert_eq!(
            chk("-leading-ok")["valid"],
            json!(true),
            "no leading-letter rule"
        );
        assert_eq!(
            chk("trailing-")["valid"],
            json!(true),
            "no trailing-hyphen rule"
        );
        assert_eq!(chk("A")["valid"], json!(true));
        assert_eq!(
            chk(&"a".repeat(255))["valid"],
            json!(true),
            "255 is the max"
        );
        // Too long / empty / disallowed characters.
        assert_eq!(chk(&"a".repeat(256))["valid"], json!(false));
        assert_eq!(chk("")["valid"], json!(false));
        for bad in ["has space", "dot.id", "slash/id", "plus+id"] {
            assert_eq!(chk(bad)["valid"], json!(false), "`{bad}` should be invalid");
        }
        // The reason is populated; `secret_id` alias; missing arg errors.
        assert!(chk("dot.id")["reason"].is_string());
        assert_eq!(
            op_valid_secret_id(json!({"secret_id": "db-password"})).unwrap()["valid"],
            json!(true)
        );
        assert!(op_valid_secret_id(json!({})).is_err());
    }

    #[test]
    fn valid_dataset_id_enforces_bigquery_naming() {
        let chk = |id: &str| op_valid_dataset_id(json!({ "id": id })).unwrap();
        // Valid: letters, digits, underscores; case is preserved; leading `_` ok.
        assert_eq!(chk("my_dataset")["valid"], json!(true));
        assert_eq!(chk("MyDataset2025")["valid"], json!(true));
        assert_eq!(
            chk("_hidden")["valid"],
            json!(true),
            "leading underscore is a hidden dataset"
        );
        assert_eq!(chk("a")["valid"], json!(true), "single char is fine");
        // 1024 chars is the max; 1025 is too long.
        assert_eq!(chk(&"a".repeat(1024))["valid"], json!(true));
        let long = chk(&"a".repeat(1025));
        assert_eq!(long["valid"], json!(false));
        assert!(long["reason"].as_str().unwrap().contains("1024"));
        // Forbidden characters: spaces, hyphens, and other specials.
        for bad in [
            "has space",
            "with-hyphen",
            "tbl.name",
            "amp&",
            "at@",
            "pct%",
        ] {
            assert_eq!(chk(bad)["valid"], json!(false), "`{bad}` should be invalid");
        }
        // Empty is rejected; `name` is an alias for `id`.
        assert_eq!(chk("")["valid"], json!(false));
        assert_eq!(
            op_valid_dataset_id(json!({"name": "events_2025"})).unwrap()["valid"],
            json!(true)
        );
        assert!(op_valid_dataset_id(json!({})).is_err());
    }

    /// Helper: call an FFI export with a JSON args object and parse the
    /// returned CString back to a `Value`, freeing the pointer. Used by the
    /// new-service error-path tests below to keep them terse.
    fn call_export(f: extern "C" fn(*const c_char) -> *const c_char, args: &Value) -> Value {
        let s = CString::new(serde_json::to_string(args).unwrap()).unwrap();
        let p = f(s.as_ptr());
        assert!(!p.is_null(), "export returned null");
        // SAFETY: p originated from CString::into_raw inside ffi_call_async.
        let parsed: Value = unsafe {
            let cs = CStr::from_ptr(p);
            serde_json::from_slice(cs.to_bytes()).expect("output JSON parses")
        };
        // SAFETY: p was returned by an export from this cdylib.
        unsafe { stryke_free_cstring(p as *mut c_char) };
        parsed
    }

    /// The Compute ops resolve `project` (from env here) BEFORE validating
    /// `zone`, and `zone` BEFORE `instance`. Pins that precedence across the
    /// list/get/start/stop/reset family — all of which share the same
    /// `resolve_project → zone → instance` prefix. A future refactor that
    /// hoisted the `instance` check above `zone` would flip the diagnostic for
    /// a caller missing both, sending them to fix the wrong argument. Every
    /// check runs before `auth_header()`, so this is deterministic without ADC.
    #[test]
    fn compute_ops_validate_project_then_zone_then_instance() {
        with_env(KEYS, || {
            std::env::set_var("GOOGLE_CLOUD_PROJECT", "test-project");
            // zone absent → "missing zone" (project resolved from env).
            for f in [
                gcp__compute_list_instances as extern "C" fn(*const c_char) -> *const c_char,
                gcp__compute_get_instance,
                gcp__compute_start_instance,
                gcp__compute_stop_instance,
                gcp__compute_reset_instance,
            ] {
                let v = call_export(f, &json!({}));
                assert_eq!(
                    v["error"].as_str(),
                    Some("missing zone"),
                    "project from env, zone absent → must report zone"
                );
            }
            // zone present, instance absent → "missing instance" for the
            // ops that take an instance (list/zones don't).
            for f in [
                gcp__compute_get_instance as extern "C" fn(*const c_char) -> *const c_char,
                gcp__compute_start_instance,
                gcp__compute_stop_instance,
                gcp__compute_reset_instance,
            ] {
                let v = call_export(f, &json!({"zone": "us-central1-a"}));
                assert_eq!(
                    v["error"].as_str(),
                    Some("missing instance"),
                    "zone supplied, instance absent → must report instance"
                );
            }
        });
        // No project source at all → project check fires first.
        with_env(KEYS, || {
            let v = call_export(gcp__compute_list_instances, &json!({"zone": "z"}));
            assert_eq!(
                v["error"].as_str(),
                Some("missing project"),
                "no project source → project check precedes zone check"
            );
        });
    }

    /// `op_logging_write_entry` requires a payload: if neither `text` nor a
    /// `json` OBJECT is supplied (project + log present), it must report
    /// "missing text or json payload" — distinct from the project/log checks.
    /// Pins that a non-object `json` value does NOT satisfy the payload
    /// requirement (the `.filter(|v| v.is_object())` guard), so a caller who
    /// passes `json => "oops"` (a string) is told the payload is missing
    /// rather than silently sending an entry with no body. Deterministic
    /// without ADC because all three checks precede `auth_header()`.
    #[test]
    fn logging_write_entry_requires_a_payload() {
        with_env(KEYS, || {
            std::env::set_var("GOOGLE_CLOUD_PROJECT", "test-project");
            // log + project present, no payload at all.
            let v = call_export(gcp__logging_write_entry, &json!({"log": "app"}));
            assert_eq!(
                v["error"].as_str(),
                Some("missing text or json payload"),
                "no text/json → payload-missing error"
            );
            // a non-object `json` is rejected (must be a structured payload).
            let v2 = call_export(
                gcp__logging_write_entry,
                &json!({"log": "app", "json": "not-an-object"}),
            );
            assert_eq!(
                v2["error"].as_str(),
                Some("missing text or json payload"),
                "non-object json must not satisfy the payload requirement"
            );
            // log absent → log check fires before the payload check.
            let v3 = call_export(gcp__logging_write_entry, &json!({"text": "hi"}));
            assert_eq!(
                v3["error"].as_str(),
                Some("missing log"),
                "log is validated before the payload"
            );
        });
    }

    /// `op_iam_test_permissions` validates `url` BEFORE `permissions`, and
    /// `permissions` must be an array. Neither check needs ADC (both precede
    /// `auth_header()`). Pins: (1) url-first precedence, (2) the array
    /// requirement is enforced — a future refactor that accepted a scalar
    /// `permissions` and wrapped it would change the contract the .stk wrapper
    /// relies on (it passes an arrayref).
    #[test]
    fn iam_test_permissions_validates_url_then_permissions_array() {
        // url absent → "missing url ..." even though permissions also absent.
        let v = call_export(gcp__iam_test_permissions, &json!({}));
        assert_eq!(
            v["error"].as_str(),
            Some("missing url (full :testIamPermissions endpoint)"),
            "url is checked first"
        );
        // url present, permissions absent → permissions error.
        let v2 = call_export(
            gcp__iam_test_permissions,
            &json!({"url": "https://example.com/x:testIamPermissions"}),
        );
        assert_eq!(
            v2["error"].as_str(),
            Some("missing permissions (array)"),
            "url supplied; permissions absent → permissions error"
        );
        // permissions present but not an array → still rejected as missing.
        let v3 = call_export(
            gcp__iam_test_permissions,
            &json!({"url": "https://example.com/x:testIamPermissions", "permissions": "scalar"}),
        );
        assert_eq!(
            v3["error"].as_str(),
            Some("missing permissions (array)"),
            "non-array permissions must be rejected"
        );
    }

    /// `op_gcs_set_iam_policy` requires `policy` to be an OBJECT, mirroring the
    /// `is_object()` guard `firestore_set` uses for `data`. Pins both the
    /// bucket-first precedence and that a non-object policy is rejected — a
    /// caller passing `policy => "{}"` (a string) must be told the policy is
    /// missing rather than have a string serialized as the request body.
    #[test]
    fn gcs_set_iam_policy_requires_object_policy() {
        // bucket absent → bucket error first.
        let v = call_export(gcp__gcs_set_iam_policy, &json!({}));
        assert_eq!(v["error"].as_str(), Some("missing bucket"));
        // bucket present, non-object policy → policy error.
        let v2 = call_export(
            gcp__gcs_set_iam_policy,
            &json!({"bucket": "b", "policy": "not-an-object"}),
        );
        assert_eq!(
            v2["error"].as_str(),
            Some("missing policy (an object)"),
            "a string policy must not satisfy the object requirement"
        );
    }

    #[test]
    fn valid_table_id_is_broader_than_dataset_id() {
        let chk = |id: &str| op_valid_table_id(json!({ "id": id })).unwrap();
        // Unlike a dataset ID, a table ID allows spaces and dashes.
        assert_eq!(chk("my-table 2025")["valid"], json!(true));
        assert_eq!(chk("orders_v2")["valid"], json!(true));
        // Unicode letters are allowed (category L); bytes counts UTF-8.
        let u = chk("café_table");
        assert_eq!(u["valid"], json!(true));
        assert_eq!(u["bytes"], json!(11)); // é is 2 bytes
                                           // The 1024-byte cap (not characters): a multibyte string near the limit.
        assert_eq!(chk(&"a".repeat(1024))["valid"], json!(true));
        let over = chk(&"a".repeat(1025));
        assert_eq!(over["valid"], json!(false));
        assert!(over["reason"].as_str().unwrap().contains("1024 bytes"));
        // Structural separators (Po category) are rejected.
        for bad in ["tbl.name", "a:b", "a/b", "amp&", "pct%"] {
            assert_eq!(chk(bad)["valid"], json!(false), "`{bad}` should be invalid");
        }
        // Empty rejected; `name` alias; missing arg errors.
        assert_eq!(chk("")["valid"], json!(false));
        assert_eq!(
            op_valid_table_id(json!({"name": "events 2025"})).unwrap()["valid"],
            json!(true)
        );
        assert!(op_valid_table_id(json!({})).is_err());
    }

    /// `truthy` must accept the three wire forms the FFI bridge can deliver for
    /// a flag (`all_users => 1` arrives as a JSON number, not a bool) and reject
    /// everything else. `bigquery_list_jobs` relies on this for `all_users`; a
    /// regression that only matched `Value::Bool` would silently scope every
    /// listing to the caller's own jobs even when the caller asked for all.
    #[test]
    fn truthy_accepts_bool_number_and_string_forms() {
        assert!(truthy(&json!(true)));
        assert!(truthy(&json!(1)));
        assert!(truthy(&json!("1")));
        assert!(truthy(&json!("true")));
        assert!(truthy(&json!("TRUE")));
        assert!(!truthy(&json!(false)));
        assert!(!truthy(&json!(0)));
        assert!(!truthy(&json!("0")));
        assert!(!truthy(&json!("no")));
        assert!(!truthy(&Value::Null));
    }

    /// The zone-scoped Compute list ops (machine_types, disks) resolve `project`
    /// (from env here) BEFORE validating `zone`, matching the existing
    /// instance/zone family. Pins that precedence so a caller missing both is
    /// pointed at the project first when no project source exists, and at the
    /// zone once a project is resolvable. Deterministic without ADC — both
    /// checks precede `auth_header()`.
    #[test]
    fn compute_zone_list_ops_validate_project_then_zone() {
        with_env(KEYS, || {
            std::env::set_var("GOOGLE_CLOUD_PROJECT", "test-project");
            for f in [
                gcp__compute_list_machine_types as extern "C" fn(*const c_char) -> *const c_char,
                gcp__compute_list_disks,
            ] {
                let v = call_export(f, &json!({}));
                assert_eq!(
                    v["error"].as_str(),
                    Some("missing zone"),
                    "project from env, zone absent → must report zone"
                );
            }
        });
        with_env(KEYS, || {
            for f in [
                gcp__compute_list_machine_types as extern "C" fn(*const c_char) -> *const c_char,
                gcp__compute_list_disks,
                gcp__compute_list_regions,
            ] {
                let v = call_export(f, &json!({"zone": "us-central1-a"}));
                assert_eq!(
                    v["error"].as_str(),
                    Some("missing project"),
                    "no project source → project check fires first"
                );
            }
        });
    }

    /// The Pub/Sub topic-scoped read ops (get_topic, topic_subscriptions) and
    /// the Secret ops (list_versions, delete) resolve `project` before their
    /// required resource arg. Pins both precedence and that the resource error
    /// message matches what the .stk wrapper's callers expect. Deterministic
    /// without ADC.
    #[test]
    fn topic_and_secret_ops_validate_project_then_resource() {
        with_env(KEYS, || {
            std::env::set_var("GOOGLE_CLOUD_PROJECT", "test-project");
            for f in [
                gcp__pubsub_get_topic as extern "C" fn(*const c_char) -> *const c_char,
                gcp__pubsub_topic_subscriptions,
            ] {
                let v = call_export(f, &json!({}));
                assert_eq!(v["error"].as_str(), Some("missing topic"));
            }
            for f in [
                gcp__secret_list_versions as extern "C" fn(*const c_char) -> *const c_char,
                gcp__secret_delete,
            ] {
                let v = call_export(f, &json!({}));
                assert_eq!(v["error"].as_str(), Some("missing secret"));
            }
        });
        with_env(KEYS, || {
            let v = call_export(gcp__pubsub_get_topic, &json!({"topic": "t"}));
            assert_eq!(
                v["error"].as_str(),
                Some("missing project"),
                "no project source → project check precedes topic check"
            );
            let v2 = call_export(gcp__secret_delete, &json!({"secret": "s"}));
            assert_eq!(v2["error"].as_str(), Some("missing project"));
        });
    }

    /// `bigquery_list_jobs` resolves `project` (the only required input) from
    /// env or opt. With no project source it must report the project gap before
    /// touching ADC. Pins that the project resolution precedes `auth_header()`.
    #[test]
    fn bigquery_list_jobs_requires_project() {
        with_env(KEYS, || {
            let v = call_export(gcp__bigquery_list_jobs, &json!({}));
            assert_eq!(v["error"].as_str(), Some("missing project"));
        });
    }
}
