use exn::{Exn, ResultExt};
use futures_core::stream::BoxStream;
use reqwest::Client;

use async_stream::try_stream;
use std::sync::Arc;

use crate::{DirMeta, Entry, Repository, error::ErrorStatus};

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

pub fn crawl<R>(
    client: Client,
    repo: Arc<R>,
    dir: DirMeta,
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
            match entry {
                Entry::File(f) => yield Entry::File(f),
                Entry::Dir(sub_dir) => {
                    yield Entry::Dir(sub_dir.clone());
                    let client = client.clone();
                    let sub_stream = crawl(client, Arc::clone(&repo), sub_dir);
                    for await item in sub_stream {
                        yield item?;
                    }
                }
            }
        }
    })
}
