use datahugger::download_with_validation;
use std::str::FromStr;
use tracing_subscriber::FmtSubscriber;

use url::Url;

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_thread_ids(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber).unwrap();
    download_with_validation(
        // 3ua2c has many files and a large file (>600M)
        &Url::from_str("https://api.osf.io/v2/nodes/3ua2c/files").unwrap(),
        "./dummy_tests",
    )
    .await
    .unwrap();
}
