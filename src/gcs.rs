//! GCS via the JSON API at `storage.googleapis.com/storage/v1`.

use std::io::{self, BufWriter};

use anyhow::{Context, Result};
use clap::Subcommand;
use serde_json::json;

use crate::common::{
    auth_headers, emit_json, emit_ndjson_line, http_client, json_request, parse_gs_uri,
    resolve_project, url_encode,
};

const BASE: &str = "https://storage.googleapis.com/storage/v1";
const UPLOAD: &str = "https://storage.googleapis.com/upload/storage/v1";

#[derive(Subcommand, Debug)]
pub enum GcsCmd {
    /// List objects under `gs://bucket/prefix`. Streams NDJSON.
    Ls {
        uri: String,
        #[arg(long)]
        delimiter: Option<String>,
        #[arg(long, default_value_t = 1000)]
        page_size: u32,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Download an object's body. `-` for stdout.
    Get {
        uri: String,
        #[arg(long, default_value = "-")]
        output: String,
    },
    /// Upload bytes. `-` for stdin.
    Put {
        uri: String,
        #[arg(long, default_value = "-")]
        input: String,
        #[arg(long, default_value = "application/octet-stream")]
        content_type: String,
        #[arg(long)]
        cache_control: Option<String>,
    },
    /// Object metadata.
    Head { uri: String },
    /// Delete an object.
    Rm { uri: String },
    /// List buckets in the configured project.
    Buckets,
}

pub async fn dispatch(project: Option<&str>, cmd: GcsCmd) -> Result<()> {
    let client = http_client()?;
    let headers = auth_headers().await?;
    match cmd {
        GcsCmd::Ls { uri, delimiter, page_size, limit } => {
            ls(&client, &headers, &uri, delimiter.as_deref(), page_size, limit).await
        }
        GcsCmd::Get { uri, output } => get(&client, &headers, &uri, &output).await,
        GcsCmd::Put { uri, input, content_type, cache_control } => {
            put(&client, &headers, &uri, &input, &content_type, cache_control.as_deref()).await
        }
        GcsCmd::Head { uri } => head(&client, &headers, &uri).await,
        GcsCmd::Rm { uri } => rm(&client, &headers, &uri).await,
        GcsCmd::Buckets => buckets(&client, &headers, project).await,
    }
}

async fn ls(
    client: &reqwest::Client,
    headers: &http::HeaderMap,
    uri: &str,
    delimiter: Option<&str>,
    page_size: u32,
    limit: Option<usize>,
) -> Result<()> {
    let (bucket, prefix) = parse_gs_uri(uri)?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    let mut token: Option<String> = None;
    let mut emitted: usize = 0;
    loop {
        let url = format!("{BASE}/b/{}/o", url_encode(&bucket));
        let mut req = client.get(&url).headers(headers.clone());
        let mut query: Vec<(String, String)> = vec![("maxResults".into(), page_size.to_string())];
        if !prefix.is_empty() {
            query.push(("prefix".into(), prefix.clone()));
        }
        if let Some(d) = delimiter {
            query.push(("delimiter".into(), d.to_string()));
        }
        if let Some(t) = &token {
            query.push(("pageToken".into(), t.clone()));
        }
        req = req.query(&query);
        let resp = json_request(req).await?;

        if let Some(prefixes) = resp.get("prefixes").and_then(|v| v.as_array()) {
            for p in prefixes {
                if let Some(s) = p.as_str() {
                    emit_ndjson_line(&mut out, &json!({ "type": "prefix", "key": s }))?;
                    emitted += 1;
                    if limit.is_some_and(|l| emitted >= l) {
                        return Ok(());
                    }
                }
            }
        }
        if let Some(items) = resp.get("items").and_then(|v| v.as_array()) {
            for obj in items {
                emit_ndjson_line(
                    &mut out,
                    &json!({
                        "type": "object",
                        "key": obj.get("name"),
                        "size": obj.get("size").and_then(|v| v.as_str()).and_then(|s| s.parse::<i64>().ok()),
                        "content_type": obj.get("contentType"),
                        "md5": obj.get("md5Hash"),
                        "etag": obj.get("etag"),
                        "updated": obj.get("updated"),
                        "storage_class": obj.get("storageClass"),
                        "generation": obj.get("generation"),
                    }),
                )?;
                emitted += 1;
                if limit.is_some_and(|l| emitted >= l) {
                    return Ok(());
                }
            }
        }
        token = resp
            .get("nextPageToken")
            .and_then(|v| v.as_str())
            .map(String::from);
        if token.is_none() {
            break;
        }
    }
    Ok(())
}

async fn get(
    client: &reqwest::Client,
    headers: &http::HeaderMap,
    uri: &str,
    output: &str,
) -> Result<()> {
    let (bucket, key) = parse_gs_uri(uri)?;
    if key.is_empty() {
        anyhow::bail!("get needs a full object URI (gs://bucket/key)");
    }
    let url = format!(
        "{BASE}/b/{}/o/{}?alt=media",
        url_encode(&bucket),
        url_encode(&key)
    );
    let resp = client
        .get(&url)
        .headers(headers.clone())
        .send()
        .await
        .context("HTTP GET")?;
    if !resp.status().is_success() {
        let st = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("HTTP {st}: {body}");
    }
    let bytes = resp.bytes().await.context("reading object body")?;
    if output == "-" {
        use tokio::io::AsyncWriteExt;
        let mut stdout = tokio::io::stdout();
        stdout.write_all(&bytes).await?;
        stdout.flush().await?;
    } else {
        tokio::fs::write(output, &bytes)
            .await
            .with_context(|| format!("writing {output}"))?;
    }
    Ok(())
}

async fn put(
    client: &reqwest::Client,
    headers: &http::HeaderMap,
    uri: &str,
    input: &str,
    content_type: &str,
    cache_control: Option<&str>,
) -> Result<()> {
    let (bucket, key) = parse_gs_uri(uri)?;
    if key.is_empty() {
        anyhow::bail!("put needs a full object URI (gs://bucket/key)");
    }
    let bytes = if input == "-" {
        use tokio::io::AsyncReadExt;
        let mut buf = Vec::new();
        tokio::io::stdin().read_to_end(&mut buf).await?;
        buf
    } else {
        tokio::fs::read(input)
            .await
            .with_context(|| format!("reading {input}"))?
    };

    let url = format!(
        "{UPLOAD}/b/{}/o?uploadType=media&name={}",
        url_encode(&bucket),
        url_encode(&key)
    );
    let mut req = client
        .post(&url)
        .headers(headers.clone())
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(bytes);
    if let Some(cc) = cache_control {
        req = req.header(reqwest::header::CACHE_CONTROL, cc);
    }
    let v = json_request(req).await?;
    emit_json(&json!({
        "bucket": v.get("bucket"),
        "key": v.get("name"),
        "size": v.get("size").and_then(|v| v.as_str()).and_then(|s| s.parse::<i64>().ok()),
        "md5": v.get("md5Hash"),
        "etag": v.get("etag"),
        "generation": v.get("generation"),
    }))
}

async fn head(
    client: &reqwest::Client,
    headers: &http::HeaderMap,
    uri: &str,
) -> Result<()> {
    let (bucket, key) = parse_gs_uri(uri)?;
    if key.is_empty() {
        anyhow::bail!("head needs a full object URI (gs://bucket/key)");
    }
    let url = format!(
        "{BASE}/b/{}/o/{}",
        url_encode(&bucket),
        url_encode(&key)
    );
    let v = json_request(client.get(&url).headers(headers.clone())).await?;
    emit_json(&json!({
        "bucket": v.get("bucket"),
        "key": v.get("name"),
        "size": v.get("size").and_then(|v| v.as_str()).and_then(|s| s.parse::<i64>().ok()),
        "content_type": v.get("contentType"),
        "cache_control": v.get("cacheControl"),
        "md5": v.get("md5Hash"),
        "etag": v.get("etag"),
        "updated": v.get("updated"),
        "storage_class": v.get("storageClass"),
        "generation": v.get("generation"),
    }))
}

async fn rm(
    client: &reqwest::Client,
    headers: &http::HeaderMap,
    uri: &str,
) -> Result<()> {
    let (bucket, key) = parse_gs_uri(uri)?;
    if key.is_empty() {
        anyhow::bail!("rm needs a full object URI (gs://bucket/key)");
    }
    let url = format!(
        "{BASE}/b/{}/o/{}",
        url_encode(&bucket),
        url_encode(&key)
    );
    let resp = client
        .delete(&url)
        .headers(headers.clone())
        .send()
        .await
        .context("HTTP DELETE")?;
    if !resp.status().is_success() {
        let st = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("HTTP {st}: {body}");
    }
    emit_json(&json!({ "bucket": bucket, "key": key, "deleted": true }))
}

async fn buckets(
    client: &reqwest::Client,
    headers: &http::HeaderMap,
    project: Option<&str>,
) -> Result<()> {
    let p = resolve_project(project)?;
    let url = format!("{BASE}/b?project={}", url_encode(&p));
    let v = json_request(client.get(&url).headers(headers.clone())).await?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    if let Some(items) = v.get("items").and_then(|v| v.as_array()) {
        for b in items {
            emit_ndjson_line(
                &mut out,
                &json!({
                    "name": b.get("name"),
                    "location": b.get("location"),
                    "storage_class": b.get("storageClass"),
                    "created": b.get("timeCreated"),
                }),
            )?;
        }
    }
    Ok(())
}
