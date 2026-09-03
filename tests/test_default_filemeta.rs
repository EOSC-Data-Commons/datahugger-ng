mod common;

use datahugger::error::ErrorStatus;
use futures_util::TryStreamExt;
use reqwest::redirect::Policy;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use url::Url;

use async_trait::async_trait;
use datahugger::crawler::{CrawlerError, ProgressManager};
use datahugger::datasets::{Arxiv, DataverseDataset, Zenodo, OSF};
use datahugger::{crawl, dabar_dataset_resolve, Dataset, Entry};
use exn::{Exn, ResultExt};
use indicatif::ProgressBar;
use reqwest_middleware::ClientWithMiddleware;

use crate::common::vcr_client;

#[async_trait]
pub trait MetaTestExt {
    async fn meta_test(
        self,
        client: &ClientWithMiddleware,
    ) -> Result<(Vec<String>, usize), Exn<CrawlerError>>;
}

#[derive(Clone)]
struct NoProgress;

impl ProgressManager for NoProgress {
    fn insert(&self, _index: usize, _pb: ProgressBar) -> ProgressBar {
        ProgressBar::hidden()
    }

    fn insert_from_back(&self, _index: usize, _pb: ProgressBar) -> ProgressBar {
        ProgressBar::hidden()
    }
}

#[async_trait]
impl MetaTestExt for Dataset {
    async fn meta_test(
        self,
        client: &ClientWithMiddleware,
    ) -> Result<(Vec<String>, usize), Exn<CrawlerError>> {
        let root_dir = self.root_dir();
        let file_count = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&file_count);
        let mp = NoProgress;
        let filenames = Arc::new(Mutex::new(Vec::new()));
        crawl(client.clone(), Arc::clone(&self.backend), root_dir, mp)
            .try_for_each_concurrent(5, |entry| {
                let counter = Arc::clone(&counter);
                let filenames = Arc::clone(&filenames);
                async move {
                    match entry {
                        Entry::Dir(_dir_meta) => {
                            // println!("{dir_meta}");
                        }
                        Entry::File(file_meta) => {
                            counter.fetch_add(1, Ordering::Relaxed);
                            dbg!(&file_meta);
                            match file_meta.filename() {
                                Some(filename) => {
                                    filenames.lock().await.push(filename.to_string());
                                }
                                None => {
                                    let filename = file_meta.file_identifier().unwrap();
                                    filenames.lock().await.push(filename.to_string());
                                }
                            }
                        }
                        Entry::Zip(_) => {
                            counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Ok(())
                }
            })
            .await
            .or_raise(|| CrawlerError {
                message: "crawl, download and validation failed".to_string(),
                status: ErrorStatus::Permanent,
            })?;
        let filenames: Vec<String> = Arc::try_unwrap(filenames)
            .expect("Arc still has multiple owners")
            .into_inner();
        Ok((filenames, file_count.load(Ordering::Relaxed)))
    }
}

#[tokio::test]
async fn arxiv() {
    // use https://arxiv.org/abs/2101.00001v1
    let client = vcr_client(reqwest::Client::new(), "arxiv_api");
    let dataset = Dataset::new(Arxiv::new("2101.00001v1"));
    let (files, n) = dataset.meta_test(&client).await.unwrap();

    assert!(files.contains(&"2101.00001v1".to_string()));
    assert_eq!(n, 1)
}

#[tokio::test]
async fn osf() {
    // use https://osf.io/5dujq/overview as test target
    let client = vcr_client(reqwest::Client::new(), "osf_api");
    let dataset = Dataset::new(OSF::new("5dujq"));
    let (_, n) = dataset.meta_test(&client).await.unwrap();

    assert_eq!(n, 4)
}

#[tokio::test]
async fn dataverse() {
    // use https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD
    let client = vcr_client(reqwest::Client::new(), "dataverse_api");
    let dataset = Dataset::new(DataverseDataset {
        id: "doi:10.7910/DVN/KBHLOD".to_string(),
        base_url: Url::from_str("https://dataverse.harvard.edu").unwrap(),
        version: ":latest-published".to_string(),
    });
    let (_, n) = dataset.meta_test(&client).await.unwrap();

    assert_eq!(n, 7)
}

#[tokio::test]
async fn zenodo() {
    // use https://zenodo.org/records/17867222
    let client = vcr_client(reqwest::Client::new(), "zenodo_api");
    let dataset = Dataset::new(Zenodo {
        id: "17867222".to_string(),
    });
    let (_, n) = dataset.meta_test(&client).await.unwrap();

    assert_eq!(n, 2)
}

#[tokio::test]
async fn dabar_resolver() {
    // use https://urn.nsk.hr/urn:nbn:hr:193:206659
    let link = "https://urn.nsk.hr/urn:nbn:hr:193:206659";
    let client = reqwest::Client::builder()
        .redirect(Policy::none())
        .build()
        .unwrap();
    let client = vcr_client(client, "dabar_resolver");
    let dataset = dabar_dataset_resolve(&client, link).await.unwrap();

    let (_, n) = dataset.meta_test(&client).await.unwrap();
    assert_eq!(n, 1)
}
