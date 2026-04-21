#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use exn::{Exn, ResultExt};
use url::Url;

use reqwest::{Client, StatusCode};
use std::{any::Any, str::FromStr};

use crate::helper::json_extract;
use crate::{
    repo::{Endpoint, FileMeta, RepoError},
    DatasetBackend, DirMeta, Entry,
};

pub struct GitHub {
    pub owner: String,
    pub repo: String,
    pub reference: String,
    pub path: Option<String>,
}

impl GitHub {
    #[must_use]
    pub fn new(
        owner: impl Into<String>,
        repo: impl Into<String>,
        reference: impl Into<String>,
        path: Option<impl Into<String>>,
    ) -> Self {
        GitHub {
            owner: owner.into(),
            repo: repo.into(),
            reference: reference.into(),
            path: path.map(|p| p.into()),
        }
    }
}

#[async_trait]
impl DatasetBackend for GitHub {
    fn root_url(&self) -> Url {
        // id for github repo is the commit hash or branch name

        // Safe to unwrap:
        // - the base URL is a hard-coded, valid absolute URL
        let mut url = Url::parse("https://api.github.com/repos").unwrap();

        {
            let mut segments = url.path_segments_mut().unwrap();
            segments.extend([&self.owner, &self.repo, "contents"]);

            if let Some(path) = &self.path {
                // split to avoid inserting slashes as a single segment
                segments.extend(path.split('/'));
            }
        }

        url.query_pairs_mut().append_pair("ref", &self.reference);

        url
    }

    async fn list(&self, client: &Client, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
        let resp = client
            .get(dir.api_url().clone())
            .send()
            .await
            .map_err(|e| RepoError {
                message: format!("HTTP GET failed: {e}"),
            })?;
        // Check status code before calling `error_for_status`
        if resp.status() == StatusCode::FORBIDDEN {
            exn::bail!(RepoError {
                message: "GitHub API rate limit excceded. \
                    You may need to provide a personal access token via the `GITHUB_TOKEN` environment variable \
                ".to_string(),
            });
        }

        let resp = resp.error_for_status().map_err(|e| RepoError {
            message: format!("HTTP error GET {}: {}", dir.api_url(), e),
        })?;

        let tree: Vec<_> = resp.json().await.map_err(|e| RepoError {
            message: format!("Failed to parse JSON from {}: {}", dir.api_url(), e),
        })?;

        let mut entries = Vec::with_capacity(tree.len());

        for (i, filej) in tree.iter().enumerate() {
            let full_path: String = json_extract(filej, "path").or_raise(|| RepoError {
                message: "Missing 'path' in tree entry".to_string(),
            })?;

            // relative to the download root
            let path_rel_to_dir = std::path::Path::new(&full_path)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or(&full_path);
            let kind: String = json_extract(filej, "type").or_raise(|| RepoError {
                message: "Missing 'type' in tree entry".to_string(),
            })?;

            match kind.as_ref() {
                "file" => {
                    let size: u64 = json_extract(filej, "size").unwrap_or(0);
                    let download_url: String =
                        json_extract(filej, "download_url").or_raise(|| RepoError {
                            message: "Missing 'download' in tree entry".to_string(),
                        })?;
                    let download_url = Url::parse(&download_url).unwrap();
                    let guess = mime_guess::from_path(path_rel_to_dir);
                    let path = dir.join(path_rel_to_dir);

                    // let sha: String = json_extract(filej, "sha").or_raise(|| RepoError {
                    //     message: "Missing 'sha' in tree entry".to_string(),
                    // })?;
                    // let checksum = Checksum::Sha1(sha);
                    let file = FileMeta::new(
                        None,
                        None,
                        path,
                        Endpoint {
                            parent_url: dir.api_url().clone(),
                            key: Some(format!("dir.{i}")),
                        },
                        download_url,
                        Some(size),
                        // didn't check git sha because what returned from url has blob as prefix
                        // in the content, I need to recalculate the sha1.
                        vec![],
                        guess.first(),
                        None,
                        None,
                        None,
                        true,
                    );
                    entries.push(Entry::File(file));
                }
                "dir" => {
                    let tree_url: String = json_extract(filej, "url").or_raise(|| RepoError {
                        message: "Missing 'url' in tree entry".to_string(),
                    })?;
                    let tree_url = Url::from_str(&tree_url).or_raise(|| RepoError {
                        message: format!("cannot parse '{tree_url}' api url"),
                    })?;
                    let dir = DirMeta::new(dir.join(path_rel_to_dir), tree_url, dir.root_url());
                    entries.push(Entry::Dir(dir));
                }
                other => {
                    exn::bail!(RepoError {
                        message: format!("Unknown tree type: {other}"),
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
