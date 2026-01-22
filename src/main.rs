use std::{fs, path::PathBuf};

use clap::{Args, Parser, Subcommand};
use datahugger::{resolve, DownloadExt};
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
            let _ = repo
                .download_with_validation(&client, dst, mp, args.limit)
                .await
                .map_err(|err| {
                    eprintln!("download failed: {err:?}");
                    std::process::exit(1);
                });
        }
    }

    Ok(())
}
