use exn::{Exn, ResultExt};
use futures_core::stream::BoxStream;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;

use async_stream::try_stream;
use std::sync::Arc;

use crate::{error::ErrorStatus, DirMeta, Entry, Repository};

#[derive(Debug)]
pub struct CrawlerError {
    pub message: String,
    pub status: ErrorStatus,
}

impl std::fmt::Display for CrawlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "crawler fail: {}", self.message)
    }
}

impl std::error::Error for CrawlerError {}

pub trait ProgressManager: Send + Sync + 'static + Clone {
    fn insert(&self, index: usize, pb: ProgressBar) -> ProgressBar;
    fn insert_from_back(&self, index: usize, pb: ProgressBar) -> ProgressBar;
}

impl ProgressManager for MultiProgress {
    fn insert(&self, index: usize, pb: ProgressBar) -> ProgressBar {
        self.insert(index, pb)
    }
    fn insert_from_back(&self, index: usize, pb: ProgressBar) -> ProgressBar {
        self.insert_from_back(index, pb)
    }
}

/// # Panics
/// indicatif template error
pub fn crawl<R>(
    client: Client,
    repo: Arc<R>,
    dir: DirMeta,
    mp: impl ProgressManager,
) -> BoxStream<'static, Result<Entry, Exn<CrawlerError>>>
where
    R: Repository + 'static + ?Sized,
{
    Box::pin(try_stream! {
        // TODO: this is at boundary need to deal with error to retry.
        let entries = repo.list(&client, dir.clone())
            .await
            .or_raise(||
                CrawlerError{
                    message: format!("cannot list all entries of '{dir}', after retry"),
                    status: ErrorStatus::Persistent,
                })?;

        for entry in entries {
            let pb = mp.insert(0, ProgressBar::new_spinner());
            pb.set_style(
                ProgressStyle::with_template("{spinner:.green} {msg}")
                    .expect("indicatif template error")
            );
            pb.enable_steady_tick(std::time::Duration::from_millis(100));
            match entry {
                Entry::File(f) => {
                    pb.set_message(format!("Crawling {}...", f.relative()));
                    yield Entry::File(f)
                }
                Entry::Dir(sub_dir) => {
                    pb.set_message(format!("Crawling {}...", sub_dir.relative()));
                    yield Entry::Dir(sub_dir.clone());
                    let client = client.clone();
                    let sub_stream = crawl(client, Arc::clone(&repo), sub_dir, mp.clone());
                    for await item in sub_stream {
                        yield item?;
                    }
                }
            }
        }
    })
}
