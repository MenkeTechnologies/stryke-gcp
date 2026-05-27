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

#[cfg(test)]
mod tests {
    use super::*;

    // ─── parse_gs_uri ────────────────────────────────────────────────

    #[test]
    fn parse_gs_uri_bucket_and_key() {
        let (b, k) = parse_gs_uri("gs://my-bucket/path/to/file.json").unwrap();
        assert_eq!(b, "my-bucket");
        assert_eq!(k, "path/to/file.json");
    }

    #[test]
    fn parse_gs_uri_bucket_only_returns_empty_key() {
        let (b, k) = parse_gs_uri("gs://bucket-only").unwrap();
        assert_eq!(b, "bucket-only");
        assert_eq!(k, "");
    }

    #[test]
    fn parse_gs_uri_trailing_slash_empty_key() {
        let (b, k) = parse_gs_uri("gs://b/").unwrap();
        assert_eq!(b, "b");
        assert_eq!(k, "");
    }

    #[test]
    fn parse_gs_uri_preserves_inner_slashes() {
        // split_once stops at FIRST '/', remainder is key verbatim.
        let (b, k) = parse_gs_uri("gs://b/a/b/c").unwrap();
        assert_eq!(b, "b");
        assert_eq!(k, "a/b/c");
    }

    #[test]
    fn parse_gs_uri_missing_scheme_errors() {
        let err = parse_gs_uri("s3://bucket/key").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("gs://"));
    }

    #[test]
    fn parse_gs_uri_case_sensitive_scheme() {
        assert!(parse_gs_uri("GS://bucket").is_err());
        assert!(parse_gs_uri("").is_err());
    }

    // ─── url_encode ──────────────────────────────────────────────────

    #[test]
    fn url_encode_passes_through_safe_chars() {
        assert_eq!(url_encode("abcXYZ123"), "abcXYZ123");
    }

    #[test]
    fn url_encode_percent_encodes_slash() {
        // The whole point of url_encode is to make path segments safe —
        // '/' MUST be percent-encoded so a nested key isn't treated as
        // additional path segments.
        assert_eq!(url_encode("a/b"), "a%2Fb");
    }

    #[test]
    fn url_encode_percent_encodes_spaces_and_special() {
        let got = url_encode("hello world?&=");
        assert!(!got.contains(' '));
        assert!(got.contains("%20") || got.contains("+"));
        // Reserved chars must be encoded.
        assert!(got.contains("%3F") || got.contains("%3f"));
    }

    #[test]
    fn url_encode_handles_unicode() {
        let got = url_encode("日本語");
        // UTF-8 encoded as % triplets — definitely no raw multi-byte chars.
        assert!(got.starts_with('%'));
    }

    // ─── resolve_project (explicit only — env-var path racy under parallel tests) ──

    #[test]
    fn resolve_project_explicit_wins() {
        // When --project is given, env vars are not consulted, so this
        // path is deterministic under parallel test execution.
        let p = resolve_project(Some("my-project")).unwrap();
        assert_eq!(p, "my-project");
    }

    #[test]
    fn resolve_project_explicit_preserved_even_if_unusual() {
        // Liberal — does not validate project ID shape.
        assert_eq!(resolve_project(Some("p1-p2")).unwrap(), "p1-p2");
        assert_eq!(resolve_project(Some("123")).unwrap(), "123");
    }

    // ─── emit_ndjson_line ────────────────────────────────────────────

    #[test]
    fn emit_ndjson_line_appends_newline() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!({"k": 1})).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "{\"k\":1}\n");
    }

    #[test]
    fn emit_ndjson_line_multi_call() {
        let mut buf = Vec::new();
        for i in 0..4 {
            emit_ndjson_line(&mut buf, &serde_json::json!({"i": i})).unwrap();
        }
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s.lines().count(), 4);
        assert!(s.ends_with('\n'));
    }

    #[test]
    fn parse_gs_uri_bucket_with_hyphens() {
        let (b, k) = parse_gs_uri("gs://my-proj-bucket/logs/2024/01/app.log").unwrap();
        assert_eq!(b, "my-proj-bucket");
        assert_eq!(k, "logs/2024/01/app.log");
    }

    #[test]
    fn url_encode_empty_string() {
        assert_eq!(url_encode(""), "");
    }

    #[test]
    fn url_encode_plus_sign() {
        assert_eq!(url_encode("+"), "%2B");
    }

    #[test]
    fn resolve_project_explicit_empty_string_allowed() {
        assert_eq!(resolve_project(Some("")).unwrap(), "");
    }

    #[test]
    fn parse_gs_uri_error_mentions_gs_scheme() {
        let err = parse_gs_uri("http://b/k").unwrap_err();
        assert!(format!("{err}").contains("gs://"));
    }

    #[test]
    fn url_encode_hash_fragment() {
        assert_eq!(url_encode("#frag"), "%23frag");
    }

    #[test]
    fn url_encode_query_string_chars() {
        let got = url_encode("a=1&b=2");
        assert!(got.contains("%3D") || got.contains("="));
        assert!(got.contains("%26") || got.contains("&"));
    }

    #[test]
    fn parse_gs_uri_numeric_bucket_name() {
        let (b, k) = parse_gs_uri("gs://12345/data").unwrap();
        assert_eq!(b, "12345");
        assert_eq!(k, "data");
    }

    #[test]
    fn emit_ndjson_line_bool_true() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!(true)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "true\n");
    }

    #[test]
    fn resolve_project_explicit_with_dots() {
        assert_eq!(resolve_project(Some("my.project.id")).unwrap(), "my.project.id");
    }

    #[test]
    fn url_encode_space_only() {
        assert_eq!(url_encode(" "), "%20");
    }

    #[test]
    fn parse_gs_uri_bucket_with_underscores() {
        let (b, k) = parse_gs_uri("gs://my_bucket/data.parquet").unwrap();
        assert_eq!(b, "my_bucket");
        assert_eq!(k, "data.parquet");
    }

    #[test]
    fn emit_ndjson_line_false_bool() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!(false)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "false\n");
    }

    #[test]
    fn url_encode_preserves_alphanumeric() {
        assert_eq!(url_encode("abc123XYZ"), "abc123XYZ");
    }

    #[test]
    fn parse_gs_uri_empty_key_after_slash() {
        let (b, k) = parse_gs_uri("gs://only-bucket/").unwrap();
        assert_eq!(b, "only-bucket");
        assert_eq!(k, "");
    }

    #[test]
    fn parse_gs_uri_bucket_only() {
        let (b, k) = parse_gs_uri("gs://bucket").unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(k, "");
    }

    #[test]
    fn parse_gs_uri_rejects_s3_scheme() {
        assert!(parse_gs_uri("s3://b/k").is_err());
    }

    #[test]
    fn url_encode_tilde_preserved() {
        assert_eq!(url_encode("~"), "~");
    }

    #[test]
    fn resolve_project_none_errors_without_env() {
        // Pin: without GOOGLE_CLOUD_PROJECT / gcloud config, None is an error.
        if std::env::var("GOOGLE_CLOUD_PROJECT").is_ok()
            || std::env::var("GCLOUD_PROJECT").is_ok()
        {
            return;
        }
        assert!(resolve_project(None).is_err());
    }

    #[test]
    fn emit_ndjson_line_number_zero() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!(0)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "0\n");
    }

    #[test]
    fn parse_gs_uri_deep_prefix() {
        let (b, k) = parse_gs_uri("gs://data/a/b/c/file.parquet").unwrap();
        assert_eq!(b, "data");
        assert_eq!(k, "a/b/c/file.parquet");
    }

    #[test]
    fn url_encode_percent_sign() {
        assert_eq!(url_encode("%"), "%25");
    }

    #[test]
    fn parse_gs_uri_error_mentions_input() {
        let bad = "file:///tmp/x";
        let err = parse_gs_uri(bad).unwrap_err();
        assert!(format!("{err}").contains(bad));
    }

    #[test]
    fn parse_gs_uri_key_with_question_mark() {
        let (b, k) = parse_gs_uri("gs://b/obj?query=1").unwrap();
        assert_eq!(b, "b");
        assert_eq!(k, "obj?query=1");
    }

    #[test]
    fn url_encode_slash_encodes() {
        assert_eq!(url_encode("/"), "%2F");
    }

    #[test]
    fn parse_gs_uri_rejects_http() {
        assert!(parse_gs_uri("http://b/k").is_err());
    }

    #[test]
    fn emit_ndjson_line_i64_zero() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!(0i64)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "0\n");
    }

    #[test]
    fn parse_gs_uri_unicode_bucket() {
        let (b, k) = parse_gs_uri("gs://バケット/キー").unwrap();
        assert_eq!(b, "バケット");
        assert_eq!(k, "キー");
    }

    #[test]
    fn url_encode_newline() {
        assert_eq!(url_encode("\n"), "%0A");
    }

    #[test]
    fn parse_gs_uri_single_segment_key() {
        let (b, k) = parse_gs_uri("gs://data/file.parquet").unwrap();
        assert_eq!(b, "data");
        assert_eq!(k, "file.parquet");
    }

    #[test]
    fn emit_ndjson_line_empty_string() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!("")).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "\"\"\n");
    }

    #[test]
    fn parse_gs_uri_key_with_ampersand() {
        let (b, k) = parse_gs_uri("gs://b/a&b").unwrap();
        assert_eq!(b, "b");
        assert_eq!(k, "a&b");
    }

    #[test]
    fn url_encode_ampersand() {
        assert_eq!(url_encode("&"), "%26");
    }

    #[test]
    fn parse_gs_uri_rejects_s3() {
        assert!(parse_gs_uri("s3://b/k").is_err());
    }

    #[test]
    fn emit_ndjson_line_negative_i64() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!(-42i64)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "-42\n");
    }

    #[test]
    fn parse_gs_uri_bucket_with_dots() {
        let (b, _) = parse_gs_uri("gs://my.company.data/out").unwrap();
        assert_eq!(b, "my.company.data");
    }

    #[test]
    fn url_encode_equals_sign() {
        assert_eq!(url_encode("="), "%3D");
    }

    #[test]
    fn parse_gs_uri_empty_scheme_rejected() {
        assert!(parse_gs_uri("notgs://b/k").is_err());
    }

    #[test]
    fn emit_ndjson_line_true_bool() {
        let mut buf = Vec::new();
        emit_ndjson_line(&mut buf, &serde_json::json!(true)).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "true\n");
    }
}
