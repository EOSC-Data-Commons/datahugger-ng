use clap::{Args, Parser, Subcommand};
use datahugger::{resolve, DownloadExt};
use indicatif::MultiProgress;
use reqwest::ClientBuilder;
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// download subcommand
    Download(DownloadArgs),
}

#[derive(Args)]
struct DownloadArgs {
    /// Url of the data record to download
    url: String,

    /// Upper limit for concurency to avaid overwhelming the network or filesystem, default to `0`
    /// which means no limitation, and it is usually fine for single dataset record.
    #[arg(short, long, default_value_t = 0)]
    limit: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_thread_ids(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    // in osf.io, '3ua2c' has many files and a large file (>600M)
    // "https://osf.io/3ua2c/",
    // "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD",
    // "https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/KBHLOD/DHJ45U",
    let cli = Cli::parse();
    match &cli.command {
        Commands::Download(args) => {
            let url = &args.url;
            let user_agent = format!("datahugger-cli/{}", env!("CARGO_PKG_VERSION"));
            let client = ClientBuilder::new().user_agent(user_agent).build()?;
            let repo = match resolve(url) {
                Ok(repo) => repo,
                Err(err) => {
                    eprintln!("failed to resolve '{url}': {err:?}");
                    std::process::exit(1);
                }
            };

            let mp = MultiProgress::new();
            let _ = repo
                .download_with_validation(&client, "./dummy_tests", mp, args.limit)
                .await
                .map_err(|err| {
                    eprintln!("download failed: {err:?}");
                    std::process::exit(1);
                });
        }
    }

    Ok(())
}
