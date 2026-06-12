use crate::helper::{json_extract, json_extract_opt};
use crate::repo::Endpoint;
use crate::{repo::RepoError, Checksum, DatasetBackend, DirMeta, Entry, FileMeta};
use async_trait::async_trait;
use exn::{Exn, ResultExt};
use reqwest::StatusCode;
use reqwest_middleware::ClientWithMiddleware;
use serde_json::Value as JsonValue;
use std::any::Any;
use std::str::FromStr;
use url::Url;

fn analyse_json(
    json: &JsonValue,
    dir: &DirMeta,
    id: &String,
) -> Result<Vec<Entry>, Exn<RepoError>> {
    let files = json.as_array().ok_or_else(|| RepoError {
        message: format!("expected array, got {:?}", json),
    })?;

    files
        .iter()
        .map(|filej| {
            let name: String = json_extract(filej, "filename").or_raise(|| RepoError {
                message: "fail to extracting 'filename' as String from json".to_string(),
            })?;

            let endpoint = Endpoint {
                parent_url: dir.api_url().clone(),
                key: Some(name.clone()),
            };

            let download_url = dir
                .api_url()
                .join(&format!(
                    "/api/rest/v1/projects/{}.1/files/{}", // https://mdposit.mddbr.eu/api/rest/docs/#/files/get_projects__projectAccessionOrID__files__filename_
                    id, name
                ))
                .or_raise(|| RepoError {
                    message: "cannot parse download base url".to_string(),
                })?;

            let size: u64 = json_extract(filej, "length").or_raise(|| RepoError {
                message: "fail to extracting 'size' as u64 from json".to_string(),
            })?;

            let hash: Option<String> = json_extract_opt(filej, "md5").or_raise(|| RepoError {
                message: "fail to extracting 'md5' as String from json".to_string(),
            })?;

            let md5_sum = match hash {
                None => vec![],
                Some(hash_str) => vec![Checksum::Md5(hash_str)],
            };

            let mime_type: String = json_extract(filej, "contentType").or_raise(|| RepoError {
                message: "fail to extracting 'contentType' as String from json".to_string(),
            })?;

            let mime_type = mime::Mime::from_str(&mime_type).or_raise(|| RepoError {
                message: format!("fail to parse '{}' to proper mime type", mime_type),
            })?;

            let mime_type = if mime_type == mime::APPLICATION_OCTET_STREAM {
                mime_guess::from_path(&name)
                    .first()
                    .unwrap_or(mime::APPLICATION_OCTET_STREAM)
            } else {
                mime_type
            };

            let upload_date = json_extract(filej, "uploadDate").or_raise(|| RepoError {
                message: "fail to extracting 'uploadDate' as String from json".to_string(),
            })?;

            let entry = FileMeta::new(
                Some(name.clone()),
                None,
                dir.join(&name),
                endpoint,
                download_url,
                Some(size),
                md5_sum,
                Some(mime_type),
                None,
                Some(upload_date),
                None,
                true,
            );

            Ok(Entry::File(entry))
        })
        .collect()
}

#[derive(Debug)]
pub struct Mdposit {
    pub id: String,
}

impl Mdposit {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Mdposit { id: id.into() }
    }
}

#[async_trait]
impl DatasetBackend for Mdposit {
    fn root_dir(&self) -> DirMeta {
        let url = Url::from_str(
            format!(
                "https://mdposit.mddbr.eu/api/rest/v1/projects/{}/filenotes",
                self.id
            )
            .as_str(),
        )
        .unwrap();

        DirMeta::new_root(&url)
    }

    async fn list(
        &self,
        client: &ClientWithMiddleware,
        dir: DirMeta,
    ) -> Result<Vec<Entry>, Exn<RepoError>> {
        let resp = client
            .get(dir.api_url().clone())
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

        let entries = analyse_json(&resp, &dir, &self.id)?;

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct MdpositJsonSrcDataset {
    pub id: String,
    pub content: String,
}

impl MdpositJsonSrcDataset {
    #[must_use]
    pub fn new(id: impl Into<String>, content: String) -> Self {
        MdpositJsonSrcDataset {
            id: id.into(),
            content,
        }
    }
}

#[async_trait]
impl DatasetBackend for MdpositJsonSrcDataset {
    fn root_dir(&self) -> DirMeta {
        let url = Url::from_str("https://mdposit.mddbr.eu").unwrap();
        DirMeta::new_root(&url)
    }

    async fn list(
        &self,
        _client: &ClientWithMiddleware,
        dir: DirMeta,
    ) -> Result<Vec<Entry>, Exn<RepoError>> {
        let json_value: JsonValue = serde_json::from_str(&self.content).or_raise(|| RepoError {
            message: "Failed to parse JSON".to_string(),
        })?;

        let entries = analyse_json(&json_value, &dir, &self.id)?;

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
