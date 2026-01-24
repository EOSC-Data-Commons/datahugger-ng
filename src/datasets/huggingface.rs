#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use exn::{Exn, OptionExt, ResultExt};
use serde_json::Value as JsonValue;
use url::Url;

use reqwest::{Client, StatusCode};
use std::any::Any;

use crate::{
    json_extract,
    repo::{Endpoint, FileMeta, RepoError},
    Checksum, DatasetBackend, DirMeta, Entry,
};

#[derive(Debug)]
pub struct HuggingFace {
    pub owner: String,
    pub repo: String,
    pub revision: String,
}

impl HuggingFace {
    #[must_use]
    pub fn new(
        owner: impl Into<String>,
        repo: impl Into<String>,
        revision: impl Into<String>,
    ) -> Self {
        HuggingFace {
            owner: owner.into(),
            repo: repo.into(),
            revision: revision.into(),
        }
    }
}

impl HuggingFace {
    fn download_url(&self, path: &str) -> Url {
        // https://huggingface.co/datasets/{repo_id}/resolve/{revision}/{path}
        let mut url = Url::parse("https://huggingface.co/datasets").unwrap();
        url.path_segments_mut()
            .unwrap()
            .extend([&self.owner, &self.repo, "resolve", &self.revision])
            .extend(path.split('/'));
        url
    }
}

#[async_trait]
impl DatasetBackend for HuggingFace {
    fn root_url(&self) -> Url {
        // https://huggingface.co/api/datasets/{owner}/{repo}/tree/{revision}/{path}
        let mut url = Url::parse("https://huggingface.co/api/datasets").unwrap();
        // safe to unwrap, we know the url.
        url.path_segments_mut()
            .unwrap()
            .extend([&self.owner, &self.repo, "tree", &self.revision]);

        url
    }

    async fn list(&self, client: &Client, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
        let resp = client
            .get(dir.api_url.clone())
            .send()
            .await
            .map_err(|e| RepoError {
                message: format!("HTTP GET failed: {e}"),
            })?;

        if resp.status() == StatusCode::FORBIDDEN {
            exn::bail!(RepoError {
                message: "Hugging Face API rate limit exceeded".to_string(),
            });
        }

        let resp = resp.error_for_status().map_err(|e| RepoError {
            message: format!("HTTP error GET {}: {e}", dir.api_url),
        })?;

        let json: JsonValue = resp.json().await.map_err(|e| RepoError {
            message: format!("Failed to parse JSON from {}: {e}", dir.api_url),
        })?;

        let files = json.as_array().ok_or_else(|| RepoError {
            message: "Expected array from Hugging Face tree API".to_string(),
        })?;

        let mut entries = Vec::with_capacity(files.len());

        for (i, filej) in files.iter().enumerate() {
            let path: String = json_extract(filej, "path").or_raise(|| RepoError {
                message: "Missing 'path'".to_string(),
            })?;
            let path = path.split('/').next_back().ok_or_raise(|| RepoError {
                message: "not get the basename of path".to_string(),
            })?;
            let kind: String = json_extract(filej, "type").or_raise(|| RepoError {
                message: "Missing 'type'".to_string(),
            })?;

            match kind.as_str() {
                "file" => {
                    let size: u64 = json_extract(filej, "size").or_raise(|| RepoError {
                        message: format!("Missing size from {}", dir.api_url),
                    })?;
                    let checksum: String = json_extract(filej, "lfs.oid")
                        .or_else(|_| json_extract(filej, "oid"))
                        .or_raise(|| RepoError {
                            message: format!("Missing 'lfs.oid' from {}", dir.api_url),
                        })?;
                    let checksum = Checksum::Sha256(checksum);
                    let path = dir.join(path);

                    let download_url = self.download_url(path.relative().as_str());

                    let file = FileMeta::new(
                        path,
                        Endpoint {
                            parent_url: dir.api_url.clone(),
                            key: Some(format!("filej.{i}")),
                        },
                        download_url,
                        Some(size),
                        vec![checksum],
                    );

                    entries.push(Entry::File(file));
                }
                "directory" => {
                    let mut api_url = dir.root_url();
                    // huggingface, path field return the relative path to the root, not to the
                    // parent folder.
                    api_url
                        .path_segments_mut()
                        .map_err(|err| RepoError {
                            message: format!("path_segments_mut fail with {err:?}"),
                        })?
                        .extend([path]);
                    let subdir = DirMeta::new(dir.join(path), api_url.clone(), api_url.clone());
                    entries.push(Entry::Dir(subdir));
                }
                other => {
                    exn::bail!(RepoError {
                        message: format!("Unknown HF entry type: {other}"),
                    });
                }
            }
        }

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
