use datahugger::download_with_validation;
use datahugger::repo_impl::{DataverseDataset, DataverseFile, OSF};
use reqwest::ClientBuilder;
use std::str::FromStr;
use std::sync::Arc;
use tracing_subscriber::FmtSubscriber;
use url::Url;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_thread_ids(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber).unwrap();
    let client = ClientBuilder::new().build().unwrap();

    // in osf.io, '3ua2c' has many files and a large file (>600M)
    download_with_validation(Arc::new(OSF::new(client.clone())), "3ua2c", "./dummy_tests")
        .await
        .unwrap();

    // doi:10.7910/DVN/KBHLOD
    let base_url = Url::from_str("https://dataverse.harvard.edu/").unwrap();
    let version = ":latest-published".to_string();
    download_with_validation(
        Arc::new(DataverseDataset::new(client.clone(), base_url, version)),
        "doi:10.7910/DVN/KBHLOD",
        "./dummy_tests",
    )
    .await
    .unwrap();

    // doi:10.7910/DVN/KBHLOD/DHJ45U
    let base_url = Url::from_str("https://dataverse.harvard.edu/").unwrap();
    let version = ":latest-published".to_string();
    download_with_validation(
        Arc::new(DataverseFile::new(client.clone(), base_url, version)),
        "doi:10.7910/DVN/KBHLOD/DHJ45U",
        "./dummy_tests",
    )
    .await
    .unwrap();
}
