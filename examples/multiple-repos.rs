use datahugger::resolve;
use futures_util::future::join_all;
use indicatif::MultiProgress;
use reqwest::ClientBuilder;
use tracing_subscriber::FmtSubscriber;

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
        // "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD",
        // "https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/KBHLOD/DHJ45U",
    ];

    let user_agent = format!("datahugger-rs-cli/{}", env!("CARGO_PKG_VERSION"));
    let client = ClientBuilder::new().user_agent(user_agent).build()?;
    let futures = repos.into_iter().map(|repo| {
        let client = client.clone();

        async move {
            let repo = match resolve(repo) {
                Ok(repo) => repo,
                Err(err) => {
                    eprintln!("failed to resolve '{repo}': {err:?}");
                    std::process::exit(1);
                }
            };

            // require:
            // use datahugger::DownloadExt;
            let mp = MultiProgress::new();
            // repo.download_with_validation(&client, "./dummy_tests", mp, 0)
            //     .await

            repo.print_meta(&client, mp).await
        }
    });

    let results = join_all(futures).await;

    for result in results {
        if let Err(err) = result {
            eprintln!("{err:?}");
        }
    }

    Ok(())
}
