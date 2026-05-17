//! Auth identity (project + ADC source).

use anyhow::{Context, Result};
use serde_json::json;

use crate::common::{emit_json, resolve_project};

pub async fn identity(project: Option<&str>) -> Result<()> {
    let p = resolve_project(project).ok();
    // Pull an access token to confirm ADC is reachable. We don't print it.
    let creds = google_cloud_auth::credentials::Builder::default()
        .build()
        .context("loading default GCP credentials")?;
    let _token = creds
        .headers(http::Extensions::new())
        .await
        .context("fetching access token")?;

    let creds_path = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok();
    emit_json(&json!({
        "project": p,
        "credentials_source": match (creds_path.as_deref(), std::env::var("GOOGLE_CLOUD_QUOTA_PROJECT").ok()) {
            (Some(_), _) => "GOOGLE_APPLICATION_CREDENTIALS file",
            _ => "Application Default Credentials chain",
        },
        "credentials_path": creds_path,
        "ok": true,
    }))
}
