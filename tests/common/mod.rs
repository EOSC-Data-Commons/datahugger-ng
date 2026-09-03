use reqwest::Client;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_vcr::{VCRMiddleware, VCRMode};
use std::path::PathBuf;

pub fn vcr_client(client: Client, cassette_name: &str) -> ClientWithMiddleware {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/fixtures");
    path.push(format!("{cassette_name}.json"));

    let mode = if std::env::var("VCR_RECORD").is_ok() {
        VCRMode::Record
    } else {
        VCRMode::Replay
    };

    let middleware = VCRMiddleware::try_from(path)
        .expect("failed to create VCR middleware")
        .with_mode(mode);

    ClientBuilder::new(client).with(middleware).build()
}
