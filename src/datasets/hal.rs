#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use exn::{Exn, ResultExt};
use serde_json::Value as JsonValue;
use url::Url;

use reqwest::{Client, StatusCode};
use std::{any::Any, str::FromStr};

use crate::helper::{json_extract, json_extract_opt};
use crate::{
    repo::{Endpoint, FileMeta, RepoError},
    DatasetBackend, DirMeta, Entry,
};

fn analyse_json(json: &JsonValue, dir: &DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
    let files = json
        .get("response")
        .and_then(|d| d.get("docs"))
        .and_then(JsonValue::as_array)
        .ok_or_else(|| RepoError {
            message: "field with key 'docs' does not resolve to an json array".to_string(),
        })?;

    let mut entries = Vec::with_capacity(files.len());

    // if not files are given, return empty vec
    if files.first().and_then(|d| d.get("files_s")).is_none() {
        return Ok(entries);
    }
    for (idx, filej) in files.iter().enumerate() {
        let endpoint = Endpoint {
            parent_url: dir.api_url(),
            key: Some(format!("files_s.{idx}")),
        };
        let download_url: String = json_extract(filej, "files_s.0").or_raise(|| RepoError {
            message: "failed to extract 'files_s.0' as String from json".to_string(),
        })?;
        let filename = download_url
            .split('/')
            .next_back()
            .ok_or_else(|| RepoError {
                message: format!("didn't get filename from '{download_url}'"),
            })?;
        let guess = mime_guess::from_path(filename);
        let download_url = Url::from_str(download_url.as_str()).or_raise(|| RepoError {
            message: format!("invalid download url '{download_url}'"),
        })?;

        let creation_date = json_extract(filej, "producedDate_tdate").or_raise(|| RepoError {
            message: "fail to extracting 'producedDate_tdate' as String from json".to_string(),
        })?;
        let last_modification_date: Option<String> = json_extract_opt(filej, "modifiedDate_tdate")
            .or_raise(|| RepoError {
                message: "fail to extracting 'modifiedDate_tdate' as String from json".to_string(),
            })?;
        let version: Option<i64> = json_extract_opt(filej, "version_i").or_raise(|| RepoError {
            message: "fail to extracting 'version_i' as String from json".to_string(),
        })?;

        let file = FileMeta::new(
            Some(filename.to_string()),
            None,
            dir.join(filename),
            endpoint,
            download_url,
            None,
            vec![],
            guess.first(),
            version.map(|v| v.to_string()),
            Some(creation_date),
            last_modification_date,
            true,
        );
        entries.push(Entry::File(file));
    }

    Ok(entries)
}

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
    fn root_dir(&self) -> DirMeta {
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
            .append_pair("fl", "halId_s,fileMain_s,files_s,fileType_s,producedDate_tdate,modifiedDate_tdate,version_i"); // https://api.archives-ouvertes.fr/docs/search/?schema=fields#fields
        DirMeta::new_root(&url)
    }

    async fn list(&self, client: &Client, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
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

#[derive(Debug)]
pub struct HalJsonSrcDataset {
    pub id: String,
    pub content: &'static str,
}

impl HalJsonSrcDataset {
    #[must_use]
    pub fn new(id: impl Into<String>, content: String) -> Self {
        HalJsonSrcDataset {
            id: id.into(),
            content: Box::leak(content.into_boxed_str()),
        }
    }
}

#[async_trait]
impl DatasetBackend for HalJsonSrcDataset {
    fn root_dir(&self) -> DirMeta {
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
            .append_pair("fl", "halId_s,fileMain_s,files_s,fileType_s,producedDate_tdate,modifiedDate_tdate,version_i");
        DirMeta::new_root(&url)
    }

    async fn list(&self, _client: &Client, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
        let json_value: JsonValue = serde_json::from_str(self.content).or_raise(|| RepoError {
            message: "Failed to parse JSON".to_string(),
        })?;

        let entries = analyse_json(&json_value, &dir)?;

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
