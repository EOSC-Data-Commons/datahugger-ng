#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use exn::{Exn, ResultExt};
use reqwest::{Client, StatusCode};
use serde_json::Value as JsonValue;
use std::{any::Any, str::FromStr};
use url::Url;

use crate::{
    helper::json_extract,
    repo::{Endpoint, FileMeta, RepoError},
    DatasetBackend, DirMeta, Entry,
};

/// A `DatasetBackend` that resolves an onedata `root_file_id` identifier
/// and lists all public files in the resulting share.
#[derive(Debug)]
pub struct OnedataDataset {
    pub domain: String,
    pub root_file_id: String,
}

impl OnedataDataset {
    #[must_use]
    pub fn new(domain: impl Into<String>, root_file_id: impl Into<String>) -> Self {
        OnedataDataset {
            domain: domain.into(),
            root_file_id: root_file_id.into(),
        }
    }
}

#[async_trait]
impl DatasetBackend for OnedataDataset {
    /// The canonical URL for the dataset — resolved once to a share URL.
    fn root_dir(&self) -> DirMeta {
        // https://demo.onedata.org/api/v3/onezone/shares/data/{file_id}
        let onezone_domain = &self.domain;
        let root_file_id = &self.root_file_id;

        let url = Url::from_str(&format!(
            "https://{onezone_domain}/api/v3/onezone/shares/data/{root_file_id}"
        ))
        .expect("invalid url");
        DirMeta::new_root(&url)
    }

    async fn list(&self, client: &Client, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
        // NOTE: Require use redirect client
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

        let kind: String = json_extract(&resp, "type").or_raise(|| RepoError {
            message: "share metadata missing 'type'".to_string(),
        })?;

        if kind != "DIR" {
            exn::bail!(RepoError {
                message: format!(
                    "{} supposed to be a 'DIR', but it is a '{}'",
                    dir.api_url(),
                    kind
                )
            })
        }

        let mut children_list_url = dir.api_url();
        children_list_url
            .path_segments_mut()
            .unwrap()
            .extend(["children"]);
        let params = [
            ("attributes", "fileId"),
            ("attributes", "name"),
            ("attributes", "type"),
            ("attributes", "size"),
        ];
        let resp = client
            .get(children_list_url)
            .query(&params)
            .send()
            .await
            .or_raise(|| RepoError {
                message: format!("fail at client sent GET {}", dir.api_url()),
            })?;

        let json: JsonValue = resp.json().await.map_err(|e| RepoError {
            message: format!("Failed to parse JSON from {}: {e}", dir.api_url()),
        })?;

        // first check if it is the last page, if not add next page as a dir like entry
        let mut is_last_page: bool = json_extract(&json, "isLast").or_raise(|| RepoError {
            message: "Expected a bool from onedata DIR children API through 'isLast' key"
                .to_string(),
        })?;
        let mut files: Vec<JsonValue> = json_extract(&json, "children").or_raise(|| RepoError {
            message: "Expected array from onedata DIR children API".to_string(),
        })?;

        // peek the paging until it is the last page
        while !is_last_page {
            let mut children_list_url = dir.api_url();
            children_list_url
                .path_segments_mut()
                .unwrap()
                .extend(["children"]);
            let next_page_token: String = json_extract(&json, "nextPageToken").or_raise(|| RepoError {
                message: "Expected a string from onedata DIR children API through 'nextPageToken' key"
                    .to_string(),
            })?;
            let params = [
                ("attributes", "fileId"),
                ("attributes", "name"),
                ("attributes", "type"),
                ("attributes", "size"),
                ("token", &next_page_token),
            ];
            let resp = client
                .get(children_list_url)
                .query(&params)
                .send()
                .await
                .or_raise(|| RepoError {
                    message: format!("fail at client sent GET {}", dir.api_url()),
                })?;

            let json: JsonValue = resp.json().await.map_err(|e| RepoError {
                message: format!("Failed to parse JSON from {}: {e}", dir.api_url()),
            })?;
            let page_files: Vec<JsonValue> =
                json_extract(&json, "children").or_raise(|| RepoError {
                    message: "Expected array from onedata DIR children API".to_string(),
                })?;
            files.extend(page_files);
            is_last_page = json_extract(&json, "isLast").or_raise(|| RepoError {
                message: "Expected a bool from onedata DIR children API through 'isLast' key"
                    .to_string(),
            })?;
        }

        let mut entries = Vec::new();
        for (idx, filej) in files.iter().enumerate() {
            let kind: String = json_extract(filej, "type").or_raise(|| RepoError {
                message: "Missing 'type'".to_string(),
            })?;
            let name: String = json_extract(filej, "name").or_raise(|| RepoError {
                message: "fail to extracting 'name' as String from json".to_string(),
            })?;
            let file_id: String = json_extract(filej, "fileId").or_raise(|| RepoError {
                message: "fail to extracting 'fileId' as String from json".to_string(),
            })?;
            let download_url = Url::parse(&format!(
                "https://{}/api/v3/onezone/shares/data/{}/content",
                self.domain, file_id
            ))
            .expect("a valid url");

            let size: u64 = json_extract(filej, "size").or_raise(|| RepoError {
                message: "fail to extracting 'attributes.size' as u64 from json".to_string(),
            })?;
            let guess = mime_guess::from_path(&name);
            let endpoint = Endpoint {
                parent_url: dir.api_url(),
                key: Some(format!("data.{idx}")),
            };
            match kind.as_str() {
                "REG" => {
                    let file = FileMeta::new(
                        None,
                        None,
                        dir.join(&name),
                        endpoint,
                        download_url,
                        Some(size),
                        vec![],
                        guess.first(),
                        None,
                        None,
                        None,
                        true,
                    );
                    entries.push(Entry::File(file));
                }
                "DIR" => {
                    let api_url = &format!(
                        "https://{}/api/v3/onezone/shares/data/{}",
                        self.domain, file_id
                    );
                    let api_url = Url::from_str(api_url).or_raise(|| RepoError {
                        message: format!("cannot parse '{api_url}' api url"),
                    })?;
                    let dir = DirMeta::new(dir.join(&name), api_url, dir.root_url());
                    entries.push(Entry::Dir(dir));
                }
                typ => {
                    exn::bail!(RepoError {
                        message: format!(
                            "kind can be 'REG' or 'DIR' for an onedata entry, got '{typ}'"
                        )
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
