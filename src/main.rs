use datahugger::download_with_validation;
use datahugger::resolve;
use reqwest::ClientBuilder;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_thread_ids(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;
    let client = ClientBuilder::new().build()?;

    // in osf.io, '3ua2c' has many files and a large file (>600M)
    // https://osf.io/3ua2c/
    let query_repo = resolve("https://osf.io/3ua2c/")?;
    // TODO: download action as method from blanket trait
    download_with_validation(&client, query_repo, "./dummy_tests").await?;

    // https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD
    let query_repo =
        resolve("https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD")?;
    download_with_validation(&client, query_repo, "./dummy_tests").await?;

    // https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/KBHLOD/DHJ45U
    let query_repo = resolve(
        "https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/KBHLOD/DHJ45U",
    )
    .unwrap();
    download_with_validation(&client, query_repo, "./dummy_tests").await?;
    Ok(())
}
