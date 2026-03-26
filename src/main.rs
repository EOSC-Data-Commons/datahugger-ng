use std::{fs, path::PathBuf};

use clap::{Args, Parser, Subcommand};
use datahugger::{resolve, DownloadExt, FileFilter};
use indicatif::MultiProgress;
use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION, USER_AGENT},
    ClientBuilder,
};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Download files of dataset
    Download(DownloadArgs),

    /// Inspect files of dataset
    Inspect(InspectArgs),
}

#[derive(Args)]
struct InspectArgs {
    /// URL of the data record to download.
    url: String,

    /// Maximum number of concurrency.
    ///
    /// This limit helps avoid overwhelming the network or filesystem.
    /// A value of `0` (the default) disables the limit.
    /// For a single dataset record, leaving this unlimited is usually fine.
    #[arg(short, long, default_value_t = 0)]
    limit: usize,

    /// Only show files matching this glob pattern.
    /// Can be specified multiple times. If omitted, all files are shown.
    #[arg(long)]
    include: Vec<String>,
}

#[derive(Args)]
struct DownloadArgs {
    /// URL of the data record to download.
    url: String,

    /// Maximum number of concurrent downloads.
    ///
    /// This limit helps avoid overwhelming the network or filesystem.
    /// A value of `0` (the default) disables the limit.
    /// For a single dataset record, leaving this unlimited is usually fine.
    #[arg(short, long, default_value_t = 0)]
    limit: usize,

    /// Destination directory for downloaded files.
    ///
    /// Defaults to the current directory (`"./"`).
    #[arg(short, long, value_name = "DIR")]
    to: Option<PathBuf>,

    /// Only download files matching this glob pattern.
    /// Can be specified multiple times. If omitted, all files are downloaded.
    #[arg(long)]
    include: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // console_subscriber::init();
    let subscriber = FmtSubscriber::builder()
        .with_thread_ids(true)
        .with_target(false)
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    // in osf.io, '3ua2c' has many files and a large file (>600M)
    // "https://osf.io/3ua2c/",
    // "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD",
    // "https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/KBHLOD/DHJ45U",
    let cli = Cli::parse();
    match cli.command {
        Commands::Download(args) => {
            let url = &args.url;
            let filter = FileFilter::from_patterns(&args.include).unwrap_or_else(|err| {
                eprintln!("invalid --include pattern: {err}");
                std::process::exit(1);
            });
            let user_agent = format!("datahugger-cli/{}", env!("CARGO_PKG_VERSION"));
            let mut headers = HeaderMap::new();
            if let Ok(token) = std::env::var("GITHUB_TOKEN") {
                headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_str(&format!("token {token}"))?,
                );
            }
            if let Ok(token) = std::env::var("DRYAD_API_TOKEN") {
                headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_str(&format!("Bearer {token}"))?,
                );
            }
            headers.insert(USER_AGENT, HeaderValue::from_str(&user_agent)?);
            let client = ClientBuilder::new()
                .user_agent(user_agent)
                .default_headers(headers)
                .use_native_tls()
                .build()?;
            let repo = match resolve(url).await {
                Ok(repo) => repo,
                Err(err) => {
                    eprintln!("failed to resolve '{url}': {err:?}");
                    std::process::exit(1);
                }
            };

            let mp = MultiProgress::new();
            let dst = args.to.unwrap_or_else(|| PathBuf::from("."));
            fs::create_dir_all(&dst)?;
            let count = repo
                .download_with_validation(&client, dst, mp, args.limit, &filter)
                .await
                .map_err(|err| {
                    eprintln!("download failed: {err:?}");
                    std::process::exit(1);
                })
                .unwrap_or(0);
            if !filter.is_accept_all() && count == 0 {
                eprintln!("warning: no files matched the --include pattern(s)");
            }
        }
        Commands::Inspect(args) => {
            let url = &args.url;
            let filter = FileFilter::from_patterns(&args.include).unwrap_or_else(|err| {
                eprintln!("invalid --include pattern: {err}");
                std::process::exit(1);
            });
            let user_agent = format!("datahugger-cli/{}", env!("CARGO_PKG_VERSION"));
            let mut headers = HeaderMap::new();
            if let Ok(token) = std::env::var("GITHUB_TOKEN") {
                headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_str(&format!("token {token}"))?,
                );
            }
            if let Ok(token) = std::env::var("DRYAD_API_TOKEN") {
                headers.insert(
                    AUTHORIZATION,
                    HeaderValue::from_str(&format!("Bearer {token}"))?,
                );
            }
            headers.insert(USER_AGENT, HeaderValue::from_str(&user_agent)?);
            let client = ClientBuilder::new()
                .user_agent(user_agent)
                .default_headers(headers)
                .use_native_tls()
                .build()?;
            let repo = match resolve(url).await {
                Ok(repo) => repo,
                Err(err) => {
                    eprintln!("failed to resolve '{url}': {err:?}");
                    std::process::exit(1);
                }
            };

            let mp = MultiProgress::new();
            let count = repo
                .print_meta(&client, mp, args.limit, &filter)
                .await
                .map_err(|err| {
                    eprintln!("inspect failed: {err:?}");
                    std::process::exit(1);
                })
                .unwrap_or(0);
            if !filter.is_accept_all() && count == 0 {
                eprintln!("warning: no files matched the --include pattern(s)");
            }
        }
    }

    Ok(())
}
