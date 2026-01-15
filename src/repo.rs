#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use futures_core::stream::BoxStream;
use serde_json::Value as JsonValue;
use url::Url;

use anyhow::anyhow;
use async_stream::try_stream;
use reqwest::Client;
use std::{path::Path, str::FromStr, sync::Arc};

use crate::json_get;

// CrawlPath track where the crawl is locating, it wraps String and provide PathBuf like methods.
#[derive(Debug, Clone)]
pub(crate) struct CrawlPath(String);

impl AsRef<Path> for CrawlPath {
    fn as_ref(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl CrawlPath {
    // concat path with the provided str into a new crawl path
    fn join(&self, p: &str) -> CrawlPath {
        let mut new_path = self.0.clone();
        if !new_path.ends_with('/') {
            new_path.push('/');
        }
        new_path.push_str(p);
        CrawlPath(new_path)
    }
}

#[derive(Debug)]
pub enum Entry {
    Dir(DirMeta),
    File(FileMeta),
}

// TODO: DirMeta and FileMeta API need consistent, FileMeta doesn't have `new`

#[derive(Debug, Clone)]
pub struct DirMeta {
    pub path: CrawlPath,
    pub api_url: Url,
}

impl DirMeta {
    pub fn new(api_url: Url, path: &str) -> Self {
        DirMeta {
            path: CrawlPath(path.to_string()),
            api_url,
        }
    }
}

#[derive(Debug)]
pub struct FileMeta {
    pub path: CrawlPath,
    pub download_url: Url,
    pub size: Option<u64>,
    pub checksum: Vec<Checksum>,
}

#[derive(Debug)]
pub enum Checksum {
    Md5(String),
    Sha256(String),
}

#[async_trait]
pub trait Repository {
    async fn list(&self, dir: DirMeta) -> anyhow::Result<Vec<Entry>>;
}

pub fn crawl<R>(repo: Arc<R>, dir: DirMeta) -> BoxStream<'static, anyhow::Result<Entry>>
where
    R: Repository + Send + Sync + 'static,
{
    Box::pin(try_stream! {
        let entries = repo.list(dir).await?;

        for entry in entries {
            match entry {
                Entry::File(f) => yield Entry::File(f),
                Entry::Dir(sub_dir) => {
                    yield Entry::Dir(sub_dir.clone());
                    let sub_stream = crawl(Arc::clone(&repo), sub_dir);
                    for await item in sub_stream {
                        yield item?;
                    }
                }
            }
        }
    })
}

// https://osf.io/
// API url at https://api.osf.io/v2/nodes/
#[derive(Debug)]
pub struct OSF {
    client: Client,
}

impl OSF {
    pub fn new(client: Client) -> Self {
        OSF { client }
    }
}

#[async_trait]
impl Repository for OSF {
    async fn list(&self, dir: DirMeta) -> anyhow::Result<Vec<Entry>> {
        let resp: JsonValue = self
            .client
            .get(dir.api_url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let files = resp
            .get("data")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| anyhow!("data not resolve to an array"))?;

        let mut entries = Vec::with_capacity(files.len());
        for filej in files {
            let name: String = json_get(filej, "attributes.name")?;
            let kind: String = json_get(filej, "attributes.kind")?;
            match kind.as_ref() {
                "file" => {
                    let size: u64 = json_get(filej, "attributes.size")?;
                    let download_link: String = json_get(filej, "links.download")?;
                    let download_link = Url::from_str(&download_link)?;
                    let hash: String = json_get(filej, "attributes.extra.hashes.sha256")?;
                    let checksum = Checksum::Sha256(hash);
                    let file = FileMeta {
                        path: dir.path.join(&name),
                        download_url: download_link,
                        size: Some(size),
                        checksum: vec![checksum],
                    };
                    entries.push(Entry::File(file));
                }
                "folder" => {
                    let rel_path = dir.path.join(&name);
                    let link: String = json_get(filej, "relationships.files.links.related.href")?;
                    let link = Url::from_str(&link)?;
                    let dir = DirMeta {
                        path: rel_path,
                        api_url: link,
                    };
                    entries.push(Entry::Dir(dir));
                }
                _ => Err(anyhow::anyhow!("kind is not 'file' or 'folder'"))?,
            }
        }

        Ok(entries)
    }
}
