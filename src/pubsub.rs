//! Pub/Sub via the REST API at `pubsub.googleapis.com/v1`.

use std::io::{self, BufWriter};

use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use clap::Subcommand;
use serde_json::{json, Value};

use crate::common::{
    auth_headers, emit_json, emit_ndjson_line, http_client, json_request, resolve_project,
};

const BASE: &str = "https://pubsub.googleapis.com/v1";

#[derive(Subcommand, Debug)]
pub enum PubSubCmd {
    /// Publish a single message to TOPIC. `--attr k=v` is repeatable.
    Publish {
        topic: String,
        #[arg(long)]
        data: String,
        #[arg(long = "attr", value_name = "K=V")]
        attrs: Vec<String>,
        #[arg(long)]
        ordering_key: Option<String>,
    },
    /// Pull messages from a subscription. `--ack` to acknowledge each.
    Pull {
        subscription: String,
        #[arg(long, default_value_t = 10)]
        max: i32,
        #[arg(long)]
        ack: bool,
        #[arg(long, default_value_t = 30)]
        deadline: i32,
    },
    /// Acknowledge one or more receipts on a subscription.
    Ack {
        subscription: String,
        /// Comma-separated ack IDs.
        #[arg(long)]
        ids: String,
    },
    /// List topics in the project.
    Topics,
    /// List subscriptions in the project.
    Subs,
}

pub async fn dispatch(project: Option<&str>, cmd: PubSubCmd) -> Result<()> {
    let client = http_client()?;
    let headers = auth_headers().await?;
    match cmd {
        PubSubCmd::Publish { topic, data, attrs, ordering_key } => {
            publish(&client, &headers, project, &topic, &data, &attrs, ordering_key.as_deref()).await
        }
        PubSubCmd::Pull { subscription, max, ack, deadline } => {
            pull(&client, &headers, project, &subscription, max, ack, deadline).await
        }
        PubSubCmd::Ack { subscription, ids } => {
            ack(&client, &headers, project, &subscription, &ids).await
        }
        PubSubCmd::Topics => topics(&client, &headers, project).await,
        PubSubCmd::Subs => subs(&client, &headers, project).await,
    }
}

/// Accepts `name`, `projects/PROJECT/topics/NAME`, or `projects/.../subscriptions/NAME`.
fn full_name(project: Option<&str>, kind: &str, raw: &str) -> Result<String> {
    if raw.starts_with("projects/") {
        return Ok(raw.to_string());
    }
    let p = resolve_project(project)?;
    Ok(format!("projects/{p}/{kind}/{raw}"))
}

fn parse_attrs(kvs: &[String]) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    for kv in kvs {
        if let Some((k, v)) = kv.split_once('=') {
            map.insert(k.to_string(), v.to_string());
        }
    }
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── full_name ───────────────────────────────────────────────────

    #[test]
    fn full_name_passes_through_fully_qualified() {
        // Already-fully-qualified names go through unchanged regardless
        // of `project` and `kind`.
        let n = full_name(None, "topics", "projects/p/topics/t").unwrap();
        assert_eq!(n, "projects/p/topics/t");
        let n = full_name(Some("ignored"), "subscriptions", "projects/X/subscriptions/Y").unwrap();
        assert_eq!(n, "projects/X/subscriptions/Y");
    }

    #[test]
    fn full_name_short_form_combined_with_explicit_project() {
        let n = full_name(Some("my-project"), "topics", "events").unwrap();
        assert_eq!(n, "projects/my-project/topics/events");
    }

    #[test]
    fn full_name_subscriptions_kind() {
        let n = full_name(Some("p"), "subscriptions", "sub-1").unwrap();
        assert_eq!(n, "projects/p/subscriptions/sub-1");
    }

    // ─── parse_attrs ─────────────────────────────────────────────────

    #[test]
    fn parse_attrs_empty() {
        let m = parse_attrs(&[]);
        assert!(m.is_empty());
    }

    #[test]
    fn parse_attrs_basic_pairs() {
        let m = parse_attrs(&["a=1".into(), "b=two".into()]);
        assert_eq!(m.get("a").map(String::as_str), Some("1"));
        assert_eq!(m.get("b").map(String::as_str), Some("two"));
    }

    #[test]
    fn parse_attrs_value_with_equals_preserved() {
        // split_once('=') splits on FIRST '=' — value can contain '='.
        let m = parse_attrs(&["jwt=a.b=c.d".into()]);
        assert_eq!(m.get("jwt").map(String::as_str), Some("a.b=c.d"));
    }

    #[test]
    fn parse_attrs_drops_malformed_silently() {
        // No '=' → silently dropped (no error path in current impl).
        let m = parse_attrs(&["a=1".into(), "no-equals".into(), "b=2".into()]);
        assert_eq!(m.len(), 2);
        assert!(!m.contains_key("no-equals"));
    }

    #[test]
    fn parse_attrs_last_wins_on_duplicate_key() {
        let m = parse_attrs(&["k=first".into(), "k=second".into()]);
        assert_eq!(m.get("k").map(String::as_str), Some("second"));
    }

    #[test]
    fn parse_attrs_empty_value() {
        let m = parse_attrs(&["k=".into()]);
        assert_eq!(m.get("k").map(String::as_str), Some(""));
    }
}

async fn publish(
    client: &reqwest::Client,
    headers: &http::HeaderMap,
    project: Option<&str>,
    topic: &str,
    data: &str,
    attrs: &[String],
    ordering_key: Option<&str>,
) -> Result<()> {
    let full = full_name(project, "topics", topic)?;
    let url = format!("{BASE}/{full}:publish");
    let mut msg = json!({
        "data": B64.encode(data.as_bytes()),
        "attributes": parse_attrs(attrs),
    });
    if let Some(ok) = ordering_key {
        msg["orderingKey"] = json!(ok);
    }
    let body = json!({ "messages": [msg] });
    let v = json_request(
        client
            .post(&url)
            .headers(headers.clone())
            .json(&body),
    )
    .await?;
    let ids: Vec<String> = v
        .get("messageIds")
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
        .unwrap_or_default();
    emit_json(&json!({
        "topic": full,
        "message_id": ids.first().cloned(),
    }))
}

async fn pull(
    client: &reqwest::Client,
    headers: &http::HeaderMap,
    project: Option<&str>,
    subscription: &str,
    max: i32,
    ack: bool,
    _deadline: i32,
) -> Result<()> {
    let full = full_name(project, "subscriptions", subscription)?;
    let url = format!("{BASE}/{full}:pull");
    let body = json!({
        "maxMessages": max,
        "returnImmediately": false,
    });
    let resp = json_request(
        client
            .post(&url)
            .headers(headers.clone())
            .json(&body),
    )
    .await?;

    let messages = resp
        .get("receivedMessages")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());

    let mut ack_ids: Vec<String> = Vec::new();
    for rm in &messages {
        let ack_id = rm.get("ackId").and_then(|v| v.as_str()).map(String::from);
        let msg = rm.get("message").cloned().unwrap_or(Value::Null);
        let data_b64 = msg.get("data").and_then(|v| v.as_str()).unwrap_or("");
        let data = B64
            .decode(data_b64)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_else(|| data_b64.to_string());
        emit_ndjson_line(
            &mut out,
            &json!({
                "ack_id": ack_id,
                "message_id": msg.get("messageId"),
                "data": data,
                "attributes": msg.get("attributes"),
                "publish_time": msg.get("publishTime"),
                "ordering_key": msg.get("orderingKey"),
            }),
        )?;
        if ack {
            if let Some(id) = ack_id {
                ack_ids.push(id);
            }
        }
    }
    if !ack_ids.is_empty() {
        let ack_url = format!("{BASE}/{full}:acknowledge");
        let _ = json_request(
            client
                .post(&ack_url)
                .headers(headers.clone())
                .json(&json!({ "ackIds": ack_ids })),
        )
        .await
        .context("acknowledge")?;
    }
    Ok(())
}

async fn ack(
    client: &reqwest::Client,
    headers: &http::HeaderMap,
    project: Option<&str>,
    subscription: &str,
    ids: &str,
) -> Result<()> {
    let full = full_name(project, "subscriptions", subscription)?;
    let url = format!("{BASE}/{full}:acknowledge");
    let ack_ids: Vec<&str> = ids.split(',').filter(|s| !s.is_empty()).collect();
    let _ = json_request(
        client
            .post(&url)
            .headers(headers.clone())
            .json(&json!({ "ackIds": ack_ids })),
    )
    .await?;
    emit_json(&json!({ "subscription": full, "acked": ack_ids.len() }))
}

async fn topics(
    client: &reqwest::Client,
    headers: &http::HeaderMap,
    project: Option<&str>,
) -> Result<()> {
    let p = resolve_project(project)?;
    let url = format!("{BASE}/projects/{p}/topics");
    let v = json_request(client.get(&url).headers(headers.clone())).await?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    if let Some(items) = v.get("topics").and_then(|v| v.as_array()) {
        for t in items {
            emit_ndjson_line(&mut out, &json!({ "name": t.get("name") }))?;
        }
    }
    Ok(())
}

async fn subs(
    client: &reqwest::Client,
    headers: &http::HeaderMap,
    project: Option<&str>,
) -> Result<()> {
    let p = resolve_project(project)?;
    let url = format!("{BASE}/projects/{p}/subscriptions");
    let v = json_request(client.get(&url).headers(headers.clone())).await?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    if let Some(items) = v.get("subscriptions").and_then(|v| v.as_array()) {
        for s in items {
            emit_ndjson_line(
                &mut out,
                &json!({
                    "name": s.get("name"),
                    "topic": s.get("topic"),
                    "ack_deadline_seconds": s.get("ackDeadlineSeconds"),
                }),
            )?;
        }
    }
    Ok(())
}
