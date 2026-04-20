#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use exn::{Exn, ResultExt};
use serde_json::Value as JsonValue;
use url::Url;

use reqwest::{Client, StatusCode};
use std::{any::Any, str::FromStr};

use crate::helper::json_extract;
use crate::{
    repo::{Endpoint, FileMeta, RepoError},
    Checksum, DatasetBackend, DirMeta, Entry,
};

fn analyse_json(json: &JsonValue, dir: &DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
    let files = json
        .get("entries")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| RepoError {
            message: "field with key '_embedded.stash:files' not resolve to an json array"
                .to_string(),
        })?;
    let mut entries = Vec::with_capacity(files.len());
    for (idx, filej) in files.iter().enumerate() {
        let endpoint = Endpoint {
            parent_url: dir.api_url(),
            key: Some(format!("entries.{idx}")),
        };
        let name: String = json_extract(filej, "key").or_raise(|| RepoError {
            message: "fail to extracting 'path' as String from json".to_string(),
        })?;
        let file_id: String = json_extract(filej, "file_id").or_raise(|| RepoError {
            message: "fail to extract 'file_id' from json".to_string(),
        })?;
        let version: String = json_extract(filej, "version_id").or_raise(|| RepoError {
            message: "fail to extract 'version_id' from json".to_string(),
        })?;
        let guess = mime_guess::from_path(&name);
        let size: u64 = json_extract(filej, "size").or_raise(|| RepoError {
            message: "fail to extracting 'size' as u64 from json".to_string(),
        })?;
        let download_url: String = json_extract(filej, "links.content").or_raise(|| RepoError {
            message: format!(
                "fail to extracting '_links.stash:download' as String from json, at parsing {}",
                dir.api_url()
            ),
        })?;
        let download_url = Url::from_str(&download_url).or_raise(|| RepoError {
            message: format!("fail to parse download_url from base_url '{download_url}'"),
        })?;
        let checksum: String = json_extract(filej, "checksum").or_raise(|| RepoError {
            message: "fail to extracting 'checksum' as String from json".to_string(),
        })?;
        let mut checksum_split = checksum.split(':');
        let checksum = match checksum_split.next() {
            Some("md5") => {
                if let Some(checksum) = checksum_split.next() {
                    Checksum::Md5(checksum.to_lowercase())
                } else {
                    exn::bail!(RepoError {
                        message: "checksum format is wrong, type md5 but no checksum".to_string()
                    })
                }
            }
            Some("sha256") => {
                if let Some(checksum) = checksum_split.next() {
                    Checksum::Sha256(checksum.to_lowercase())
                } else {
                    exn::bail!(RepoError {
                        message: "checksum format is wrong, type sha256 but no checksum"
                            .to_string()
                    })
                }
            }
            _ => exn::bail!(RepoError {
                message: "checksum field is wrong".to_string()
            }),
        };
        let created: String = json_extract(filej, "created").or_raise(|| RepoError {
            message: "fail to extracting 'created' as String from json".to_string(),
        })?;
        let updated: String = json_extract(filej, "updated").or_raise(|| RepoError {
            message: "fail to extracting 'updated' as String from json".to_string(),
        })?;
        let file = FileMeta::new(
            Some(name.clone()),
            Some(file_id),
            dir.join(&name),
            endpoint,
            download_url,
            Some(size),
            vec![checksum],
            guess.first(),
            Some(version),
            Some(created),
            Some(updated),
            true,
        );
        entries.push(Entry::File(file));
    }

    Ok(entries)
}

// https://zenodo.org/
// API root url at https://zenodo.org/api/
//
// Zenodo use flatten folder tree structure, all files with nexted parent folder are list in one
// API call.
#[derive(Debug)]
pub struct Zenodo {
    pub id: String,
}

impl Zenodo {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Zenodo { id: id.into() }
    }
}

#[allow(clippy::too_many_lines)]
#[async_trait]
impl DatasetBackend for Zenodo {
    fn root_dir(&self) -> DirMeta {
        // https://zenodo.org/api/<id> to start for every dateset entry

        // Safe to unwrap:
        // - the base URL is a hard-coded, valid absolute URL
        // - `path_segments_mut` cannot fail for this URL scheme
        let mut url = Url::from_str("https://zenodo.org/api/records").unwrap();
        url.path_segments_mut().unwrap().extend([&self.id, "files"]);
        DirMeta::new_root(&url)
    }

    async fn list(&self, client: &Client, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
        // NOTE: for dev, the first entry point url for the `dir.api_url` is the `root_dir` (from `root_url`) of the Dataset
        let resp = client
            .get(dir.api_url())
            .send()
            .await
            .or_raise(|| RepoError {
                message: format!("fail at client sent GET {}", dir.api_url()),
            })?;
        let resp = resp.error_for_status().map_err(|err| match err.status() {
            Some(StatusCode::NOT_FOUND) => RepoError {
                message: format!("resource not found when GET {}", dir.api_url()),
            },
            Some(status_code) => RepoError {
                message: format!(
                    "fail GET {}, with state code: {}",
                    dir.api_url(),
                    status_code.as_str()
                ),
            },
            None => RepoError {
                message: format!("fail GET {}, network / protocol error", dir.api_url(),),
            },
        })?;
        let resp: JsonValue = resp.json().await.or_raise(|| RepoError {
            message: format!("fail GET {}, unable to convert to json", dir.api_url(),),
        })?;

        let entries = analyse_json(&resp, &dir)?;

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct ZenodoJsonSrcDataset {
    pub id: String,
    pub content: String,
}

impl ZenodoJsonSrcDataset {
    #[must_use]
    pub fn new(id: impl Into<String>, content: String) -> Self {
        ZenodoJsonSrcDataset {
            id: id.into(),
            content,
        }
    }
}

#[async_trait]
impl DatasetBackend for ZenodoJsonSrcDataset {
    fn root_dir(&self) -> DirMeta {
        // https://zenodo.org/api/<id> to start for every dateset entry

        // Safe to unwrap:
        // - the base URL is a hard-coded, valid absolute URL
        // - `path_segments_mut` cannot fail for this URL scheme
        let mut url = Url::from_str("https://zenodo.org/api/records").unwrap();
        url.path_segments_mut().unwrap().extend([&self.id, "files"]);
        DirMeta::new_root(&url)
    }

    async fn list(&self, _client: &Client, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
        let json_value: JsonValue = serde_json::from_str(&self.content).or_raise(|| RepoError {
            message: "Failed to parse JSON".to_string(),
        })?;

        let entries = analyse_json(&json_value, &dir)?;

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
