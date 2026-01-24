#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use exn::{Exn, ResultExt};
use serde_json::Value as JsonValue;
use url::Url;

use reqwest::{Client, StatusCode};
use std::{any::Any, str::FromStr};

use crate::{
    json_extract,
    repo::{Endpoint, FileMeta, RepoError},
    DatasetBackend, DirMeta, Entry,
};

pub struct GitHub {
    pub owner: String,
    pub repo: String,
    pub branch_or_commit: String,
}

impl GitHub {
    #[must_use]
    pub fn new(
        owner: impl Into<String>,
        repo: impl Into<String>,
        branch_or_commit: impl Into<String>,
    ) -> Self {
        GitHub {
            owner: owner.into(),
            repo: repo.into(),
            branch_or_commit: branch_or_commit.into(),
        }
    }
}

fn github_branch_or_commit_from_url(url: &Url) -> Option<String> {
    let segments: Vec<&str> = url.path_segments()?.collect();

    // GitHub tree URL format:
    // ["repos", "owner", "repo", "git", "trees", "<branch_or_commit>"]
    //https://api.github.com/repos/rs4rse/vizmat/git/trees/main?recursive=1
    if segments.len() >= 6 && segments[3] == "git" && segments[4] == "trees" {
        Some(segments[5].to_string())
    } else {
        None
    }
}

#[async_trait]
impl DatasetBackend for GitHub {
    fn root_url(&self) -> Url {
        // id for github repo is the commit hash or branch name

        // Safe to unwrap:
        // - the base URL is a hard-coded, valid absolute URL
        let mut url = Url::parse("https://api.github.com/repos").unwrap();
        url.path_segments_mut().unwrap().extend([
            &self.owner,
            &self.repo,
            "git",
            "trees",
            &self.branch_or_commit,
        ]);
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
        // Check status code before calling `error_for_status`
        if resp.status() == StatusCode::FORBIDDEN {
            exn::bail!(RepoError {
                message: "GitHub API rate limit excceded. \
                    You may need to provide a personal access token via the `GITHUB_TOKEN` environment variable \
                ".to_string(),
            });
        }

        let resp = resp.error_for_status().map_err(|e| RepoError {
            message: format!("HTTP error GET {}: {}", dir.api_url, e),
        })?;

        let json: JsonValue = resp.json().await.map_err(|e| RepoError {
            message: format!("Failed to parse JSON from {}: {}", dir.api_url, e),
        })?;

        let tree = json
            .get("tree")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| RepoError {
                message: "No 'tree' field in GitHub API response".to_string(),
            })?;

        let mut entries = Vec::with_capacity(tree.len());

        for (i, filej) in tree.iter().enumerate() {
            let path: String = json_extract(filej, "path").or_raise(|| RepoError {
                message: "Missing 'path' in tree entry".to_string(),
            })?;
            let kind: String = json_extract(filej, "type").or_raise(|| RepoError {
                message: "Missing 'type' in tree entry".to_string(),
            })?;

            let record_id = github_branch_or_commit_from_url(&dir.root_url())
                .expect("can parse branch or commit from url");
            match kind.as_ref() {
                "blob" => {
                    let size: u64 = json_extract(filej, "size").unwrap_or(0);
                    let path = dir.join(&path);
                    let download_url = format!(
                        "https://raw.githubusercontent.com/{}/{}/{}/{}",
                        self.owner,
                        self.repo,
                        record_id,
                        path.relative()
                    );
                    let download_url = Url::parse(&download_url).unwrap();

                    let file = FileMeta::new(
                        path,
                        Endpoint {
                            parent_url: dir.api_url.clone(),
                            key: Some(format!("tree.{i}")),
                        },
                        download_url,
                        Some(size),
                        vec![],
                    );
                    entries.push(Entry::File(file));
                }
                "tree" => {
                    let tree_url: String = json_extract(filej, "url").or_raise(|| RepoError {
                        message: "Missing 'url' in tree entry".to_string(),
                    })?;
                    let tree_url = Url::from_str(&tree_url).or_raise(|| RepoError {
                        message: format!("cannot parse '{tree_url}' api url"),
                    })?;
                    let dir = DirMeta::new(dir.join(&path), tree_url, dir.root_url());
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
