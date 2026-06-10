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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

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
}
