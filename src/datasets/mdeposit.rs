use crate::helper::json_extract;
use crate::repo::Endpoint;
use crate::{repo::RepoError, DatasetBackend, DirMeta, Entry, FileMeta};
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
        .enumerate()
        .map(|(idx, filej)| {
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
                    "/api/rest/current/projects/{}.1/files/{}",
                    id, name
                ))
                .or_raise(|| RepoError {
                    message: "cannot parse download base url".to_string(),
                })?;

            let entry = FileMeta::new(
                Some(name),
                None,
                dir.join(""),
                endpoint,
                download_url,
                None,
                vec![],
                None,
                None,
                None,
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
