use crate::{repo::RepoError, DatasetBackend, DirMeta, Entry};
use async_trait::async_trait;
use exn::{Exn, ResultExt};
use reqwest::{Client, StatusCode};
use serde_json::Value as JsonValue;
use std::any::Any;
use std::str::FromStr;
use url::Url;

#[derive(Debug)]
pub struct SwissUbase {
    pub id: String,
}

impl SwissUbase {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        SwissUbase { id: id.into() }
    }
}

#[async_trait]
impl DatasetBackend for SwissUbase {
    fn root_dir(&self) -> DirMeta {
        let url = Url::from_str(
            format!("https://www.swissubase.ch/api/v2/datasets/{}", self.id).as_str(),
        )
        .unwrap();
        DirMeta::new_root(&url)
    }

    async fn list(&self, client: &Client, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
        println!("dir {}", dir);

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

        println!("resp: {:?}", resp);

        let mut vec = Vec::new();

        Ok(vec)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CrawlPath;

    #[tokio::test]
    async fn test_list() {
        let uuid = "41d06629-9e03-4631-933f-ff783a4d394c";

        let dataset = SwissUbase::new(uuid);
        let client = Client::new();

        let entries = dataset.list(&client, dataset.root_dir()).await.unwrap();
    }
}
