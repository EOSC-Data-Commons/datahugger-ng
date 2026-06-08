use std::any::Any;
use async_trait::async_trait;
use exn::{Exn, ResultExt};
use crate::{repo::RepoError,DatasetBackend, DirMeta, Entry};
use serde_json::Value as JsonValue;
use url::Url;
use std::str::FromStr;
use reqwest::StatusCode;
use reqwest_middleware::ClientWithMiddleware;


fn analyse_json(json: &JsonValue, dir: &DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {

    println!("analysing {}", json);

    Ok(vec![])
}

#[derive(Debug)]
pub struct Mdeposit {
    pub id: String,
}

impl Mdeposit {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Mdeposit { id: id.into() }
    }
}

#[async_trait]
impl DatasetBackend for Mdeposit {

    fn root_dir(&self) -> DirMeta {
        let url = Url::from_str(
            format!("https://mdposit.mddbr.eu/api/rest/v1/projects/{}/filenotes", self.id).as_str(),
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

        let entries = analyse_json(&resp, &dir)?;

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

}