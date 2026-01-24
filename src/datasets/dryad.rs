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
    Checksum, DatasetBackend, DirMeta, Entry,
};

// https://datadryad.org/
// API root url at https://datadryad.org/api/v2
#[derive(Debug)]
pub struct DataDryad {
    pub id: String,
    base_url: Url,
}

impl DataDryad {
    #[must_use]
    pub fn new(id: impl Into<String>, base_url: &Url) -> Self {
        DataDryad {
            id: id.into(),
            base_url: base_url.clone(),
        }
    }
}

#[allow(clippy::too_many_lines)]
#[async_trait]
impl DatasetBackend for DataDryad {
    fn root_url(&self) -> Url {
        // https://datadryad.org/api/v2/datasets/<id> to start for every dateset entry

        // Safe to unwrap:
        // - the base URL is a hard-coded, valid absolute URL
        // - `path_segments_mut` cannot fail for this URL scheme
        let mut url = Url::from_str("https://datadryad.org/api/v2/datasets").unwrap();
        url.path_segments_mut().unwrap().extend([&self.id]);
        url
    }

    async fn list(&self, client: &Client, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
        let resp = client
            .get(dir.api_url.clone())
            .send()
            .await
            .or_raise(|| RepoError {
                message: format!("fail at client sent GET {}", dir.api_url),
            })?;
        let resp = resp.error_for_status().map_err(|err| match err.status() {
            Some(StatusCode::NOT_FOUND) => RepoError {
                message: format!("resource not found when GET {}", dir.api_url),
            },
            Some(status_code) => RepoError {
                message: format!(
                    "fail GET {}, with state code: {}",
                    dir.api_url,
                    status_code.as_str()
                ),
            },
            None => RepoError {
                message: format!("fail GET {}, network / protocol error", dir.api_url,),
            },
        })?;
        let resp: JsonValue = resp.json().await.or_raise(|| RepoError {
            message: format!("fail GET {}, unable to convert to json", dir.api_url,),
        })?;

        // get link to the api of latest version of dataset
        let version: String =
            json_extract(&resp, "_links.stash:version.href").or_raise(|| RepoError {
                message: "fail to extract '_links.stash:version.href' as string from json"
                    .to_string(),
            })?;

        // second http GET call to get files
        // safe to unwrap: because base_url is from url.
        let mut files_api_url = self.base_url.join(&version).or_raise(|| RepoError {
            message: format!(
                "cannot join version '{}' to base url '{}'",
                version,
                self.base_url.as_str()
            ),
        })?;
        files_api_url
            .path_segments_mut()
            .expect("url cannot be base")
            .extend(["files"]);
        let resp = client
            .get(files_api_url.clone())
            .send()
            .await
            .or_raise(|| RepoError {
                message: format!("fail at client sent GET {files_api_url}"),
            })?;
        let resp = resp.error_for_status().map_err(|err| match err.status() {
            Some(StatusCode::NOT_FOUND) => RepoError {
                message: format!("resource not found when GET {files_api_url}"),
            },
            Some(status_code) => RepoError {
                message: format!(
                    "fail GET {}, with state code: {}",
                    dir.api_url,
                    status_code.as_str()
                ),
            },
            None => RepoError {
                message: format!("fail GET {files_api_url}, network / protocol error"),
            },
        })?;
        let resp: JsonValue = resp.json().await.or_raise(|| RepoError {
            message: format!("fail GET {files_api_url}, unable to convert to json"),
        })?;

        let files = resp
            .get("_embedded")
            .and_then(|d| d.get("stash:files"))
            .and_then(JsonValue::as_array)
            .ok_or_else(|| RepoError {
                message: "field with key '_embedded.stash:files' not resolve to an json array"
                    .to_string(),
            })?;
        let mut entries = Vec::with_capacity(files.len());
        for (idx, filej) in files.iter().enumerate() {
            let endpoint = Endpoint {
                parent_url: files_api_url.clone(),
                key: Some(format!("_embedded.stash:files.{idx}")),
            };
            let name: String = json_extract(filej, "path").or_raise(|| RepoError {
                message: "fail to extracting 'path' as String from json".to_string(),
            })?;
            let size: u64 = json_extract(filej, "size").or_raise(|| RepoError {
                message: "fail to extracting 'size' as u64 from json".to_string(),
            })?;
            let download_url_path: String =
                json_extract(filej, "_links.stash:download.href").or_raise(|| RepoError {
                   message: format!("fail to extracting '_links.stash:download' as String from json, at parsing {files_api_url}")
                })?;
            let download_url = self
                .base_url
                .join(&download_url_path)
                .or_raise(|| RepoError {
                    message: format!(
                        "fail to concat download_url from base_url '{}', and path '{}'",
                        self.base_url.as_str(),
                        download_url_path
                    ),
                })?;
            let hash_type: String = json_extract(filej, "digestType").or_raise(|| RepoError {
                message: "fail to extracting 'digestType' as String from json".to_string(),
            })?;
            let checksum = if hash_type.to_lowercase() == "md5" {
                let hash: String = json_extract(filej, "digest").or_raise(|| RepoError {
                    message:
                        "fail to extracting 'attributes.extra.hashes.sha256' as String from json"
                            .to_string(),
                })?;
                Checksum::Md5(hash)
            } else {
                exn::bail!(RepoError {
                    message: format!("unsupported hash type, '{hash_type}'")
                })
            };
            let file = FileMeta::new(
                dir.join(&name),
                endpoint,
                download_url,
                Some(size),
                vec![checksum],
            );
            entries.push(Entry::File(file));
        }

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
