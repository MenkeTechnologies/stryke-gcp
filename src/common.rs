//! Shared plumbing: ADC token fetcher, REST HTTP client builder, helpers.

use std::io::{self, BufWriter, Write};

use anyhow::{anyhow, Context, Result};
use http::HeaderMap;
use reqwest::Client;

pub fn http_client() -> Result<Client> {
    Client::builder()
        .user_agent("stryke-gcp-helper/0.1")
        .build()
        .context("building HTTP client")
}

/// Resolve a project ID from explicit flag or env.
pub fn resolve_project(explicit: Option<&str>) -> Result<String> {
    if let Some(p) = explicit {
        return Ok(p.to_string());
    }
    for var in ["GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GCP_PROJECT"] {
        if let Ok(v) = std::env::var(var) {
            if !v.is_empty() {
                return Ok(v);
            }
        }
    }
    Err(anyhow!(
        "no GCP project specified — pass --project or set $GOOGLE_CLOUD_PROJECT"
    ))
}

/// Fetch an `Authorization: Bearer <token>` header set from Application
/// Default Credentials.
pub async fn auth_headers() -> Result<HeaderMap> {
    use google_cloud_auth::credentials::{Builder, CacheableResource};
    let creds = Builder::default()
        .build()
        .context("loading default GCP credentials")?;
    let ext = http::Extensions::new();
    let cacheable = creds
        .headers(ext)
        .await
        .context("fetching access token")?;
    match cacheable {
        CacheableResource::New { data, .. } => Ok(data),
        CacheableResource::NotModified => {
            Err(anyhow!("credentials returned NotModified without a fresh header set"))
        }
    }
}

pub fn emit_json<T: serde::Serialize>(v: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

pub fn emit_ndjson_line<T: serde::Serialize, W: Write>(w: &mut W, v: &T) -> Result<()> {
    serde_json::to_writer(&mut *w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

/// `gs://bucket/key/path` → `("bucket", "key/path")`.
pub fn parse_gs_uri(uri: &str) -> Result<(String, String)> {
    let rest = uri
        .strip_prefix("gs://")
        .ok_or_else(|| anyhow!("expected `gs://bucket/key…`, got `{uri}`"))?;
    if let Some((bucket, key)) = rest.split_once('/') {
        Ok((bucket.to_string(), key.to_string()))
    } else {
        Ok((rest.to_string(), String::new()))
    }
}

/// Percent-encode for URL path segments.
pub fn url_encode(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

/// Run a reqwest request, check status, return parsed JSON or raw text on
/// non-JSON success.
pub async fn json_request(req: reqwest::RequestBuilder) -> Result<serde_json::Value> {
    let resp = req.send().await.context("HTTP request")?;
    let status = resp.status();
    let text = resp.text().await.context("reading response")?;
    if !status.is_success() {
        anyhow::bail!("HTTP {}: {}", status, text);
    }
    if text.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_str(&text).with_context(|| format!("parsing JSON response: {text}"))
}
