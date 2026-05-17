//! `stryke-gcp-helper` — bridge binary for the stryke `gcp` package.
//!
//! Subcommands wrap the official google-cloud-rust crates. Output is JSON
//! (single object) or NDJSON (lists / streams). Authentication uses
//! Application Default Credentials: env vars → `gcloud auth
//! application-default login` → metadata server.
//!
//! v1 ships GCS + Pub/Sub. BigQuery is queued for a v2 that uses a
//! REST-based path (the official Rust BQ crate currently pulls in an
//! arrow-arith + chrono version conflict).

use anyhow::Result;
use clap::{Parser, Subcommand};

mod auth;
mod common;
mod gcs;
mod pubsub;

#[derive(Parser, Debug)]
#[command(
    name = "stryke-gcp-helper",
    version,
    about = "Google Cloud bridge (GCS, Pub/Sub) for the stryke `gcp` package"
)]
struct Cli {
    /// GCP project ID. Defaults to $GOOGLE_CLOUD_PROJECT / gcloud config.
    #[arg(long, short = 'p', env = "GOOGLE_CLOUD_PROJECT", global = true)]
    project: Option<String>,

    #[command(subcommand)]
    cmd: Top,
}

#[derive(Subcommand, Debug)]
enum Top {
    /// Cloud Storage — buckets and objects.
    #[command(subcommand)]
    Gcs(gcs::GcsCmd),

    /// Pub/Sub — publish, pull, topics, subscriptions.
    #[command(subcommand)]
    Pubsub(pubsub::PubSubCmd),

    /// Identity / credentials check.
    Auth,

    /// Alias for `auth` — exits 0 if ADC credentials are reachable.
    Ping,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("stryke-gcp-helper: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let project = cli.project.as_deref();
    match cli.cmd {
        Top::Gcs(c) => gcs::dispatch(project, c).await,
        Top::Pubsub(c) => pubsub::dispatch(project, c).await,
        Top::Auth | Top::Ping => auth::identity(project).await,
    }
}
