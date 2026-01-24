#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use exn::{Exn, ResultExt};
use serde_json::Value as JsonValue;
use url::Url;

use reqwest::{Client, StatusCode};
use std::{any::Any, str::FromStr};

use crate::{
    repo::{Endpoint, FileMeta, RepoError},
    DatasetBackend, DirMeta, Entry,
};

// https://hal.science/
// API root url at https://hal.science/<id>?
#[derive(Debug)]
pub struct HalScience {
    pub id: String,
}

impl HalScience {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        HalScience { id: id.into() }
    }
}

#[async_trait]
impl DatasetBackend for HalScience {
    fn root_url(&self) -> Url {
        // HAL Search API endpoint
        // can get files of a record by following search api call, e.g. for 'cel-01830944'
        // curl "https://api.archives-ouvertes.fr/search/?q=halId_s:cel-01830943&wt=json&fl=halId_s,fileMain_s,files_s,fileType_s"
        //
        // it returns
        // {
        //   "response":{
        //     "numFound":1,
        //     "start":0,
        //     "maxScore":5.930896,
        //     "numFoundExact":true,
        //     "docs":[{
        //       "halId_s":"cel-01830944",
        //       "fileMain_s":"https://hal.science/cel-01830944/document",
        //       "files_s":["https://hal.science/cel-01830944/file/MAILLOT_Cours_inf340-systemes_information.pdf"],
        //       "fileType_s":["file"]
        //     }]
        //   }
        // }⏎
        //

        // Safe to unwrap:
        // - the base URL is a hard-coded, valid absolute URL
        let mut url = Url::from_str("https://api.archives-ouvertes.fr/search/").unwrap();

        url.query_pairs_mut()
            .append_pair("q", &format!("halId_s:{}", self.id))
            .append_pair("wt", "json")
            .append_pair("fl", "halId_s,fileMain_s,files_s,fileType_s");

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
        let files = resp
            .get("response")
            .and_then(|d| d.get("docs"))
            .and_then(|docs| docs.get(0))
            .and_then(|d| d.get("files_s"))
            .and_then(JsonValue::as_array)
            .ok_or_else(|| RepoError {
                message: "field with key 'data' not resolve to an json array".to_string(),
            })?;

        let mut entries = Vec::with_capacity(files.len());
        for (idx, filej) in files.iter().enumerate() {
            let endpoint = Endpoint {
                parent_url: dir.api_url.clone(),
                key: Some(format!("response.docs.0.files_s.{idx}")),
            };
            let JsonValue::String(download_url) = filej else {
                todo!()
            };
            let filename = download_url
                .split('/')
                .next_back()
                .ok_or_else(|| RepoError {
                    message: format!("didn't get filename from '{download_url}'"),
                })?;
            let download_url = Url::from_str(download_url).or_raise(|| RepoError {
                message: format!("invalid download url '{download_url}'"),
            })?;
            let file = FileMeta::new(
                dir.join(&format!("{filename}.pdf")),
                endpoint,
                download_url,
                None,
                vec![],
            );
            entries.push(Entry::File(file));
        }

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
