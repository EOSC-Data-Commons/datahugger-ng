use datahugger::download_with_validation;
use datahugger::resolve;
use exn::ResultExt;
use reqwest::ClientBuilder;
use tracing_subscriber::FmtSubscriber;

#[derive(Debug)]
enum AppError {
    Fatal { consequences: &'static str },
    // Trivial,
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AppError::Fatal { consequences } => write!(f, "fatal error: {consequences}"),
            // AppError::Trivial => write!(f, "trivial error"),
        }
    }
}

impl std::error::Error for AppError {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_thread_ids(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;
    let repos = [
        // in osf.io, '3ua2c' has many files and a large file (>600M)
        "https://osf.io/3ua2c/",
        "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD",
        "https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/KBHLOD/DHJ45U",
    ];

    let user_agent = format!("datahugger-rs-cli/{}", env!("CARGO_PKG_VERSION"));
    let client = ClientBuilder::new().user_agent(user_agent).build()?;
    for repo in repos {
        let repo = match resolve(repo) {
            Ok(repo) => repo,
            Err(err) => {
                eprintln!("failed to resolve '{repo}': {err:?}");
                std::process::exit(1);
            }
        };

        // TODO: download action as method from blanket trait
        if let Err(err) = download_with_validation(&client, repo, "./dummy_tests")
            .await
            .or_raise(|| AppError::Fatal {
                consequences: "download fail",
            })
        {
            eprintln!("{err:?}");
        }
    }
    Ok(())
}
