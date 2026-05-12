use crate::helper::json_extract;
use crate::repo::Endpoint;
use crate::{repo::RepoError, Checksum, DatasetBackend, DirMeta, Entry, FileMeta};
use async_trait::async_trait;
use exn::{Exn, ResultExt};
use reqwest::{Client, StatusCode};
use serde_json::Value as JsonValue;
use std::any::Any;
use std::path::Path;
use std::str::FromStr;
use url::Url;

fn analyse_json(json: &JsonValue, dir: &DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
    let files = json
        .get("flatFileStructure")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| RepoError {
            message: "field with key 'flatFileStructure' does not resolve to an json array"
                .to_string(),
        })?;

    let license: String = json_extract(json, "usageLicense.iconCode").or_raise(|| RepoError {
        message: "fail to extracting 'dataFile.filename' as String from json".to_string(),
    })?; // json.get("usageLicense").and_then(|d| d.get("iconCode")).and_then(JsonValue::as_str);

    let checksum: String = json_extract(json, "md5").or_raise(|| RepoError {
        message: "fail to extracting 'usageLicense.iconCode' as String from json".to_string(),
    })?;

    let download_url = format!("{}/download", dir.api_url());

    let download_url = Url::from_str(download_url.as_str()).or_raise(|| RepoError {
        message: format!("invalid download url '{download_url}'"),
    })?;

    let checksum = Checksum::Md5(checksum);

    let publication_date: String =
        json_extract(json, "publicationDate").or_raise(|| RepoError {
            message: "fail to extracting 'publicationDate' as String from json".to_string(),
        })?;

    let major_version: u64 =
        json_extract(json, "versionNumber.majorVersion").or_raise(|| RepoError {
            message: "fail to extracting 'versionNumber.majorVersion' as String from json"
                .to_string(),
        })?;

    let minor_version: u64 =
        json_extract(json, "versionNumber.minorVersion").or_raise(|| RepoError {
            message: "fail to extracting 'versionNumber.minorVersion' as String from json"
                .to_string(),
        })?;

    let version: String = format!("{}.{}", major_version, minor_version);

    let mut entries = Vec::with_capacity(files.len());

    let files: Vec<&str> = files
        .iter()
        .filter_map(|filej| {
            let s = filej.as_str()?; // or however you extract the string from your JSON value
            let s = s.trim_end_matches('/');
            let path = Path::new(s);
            // Keep only paths that have a non-empty file extension
            path.extension()?;
            path.file_name()?.to_str()
        })
        .collect();

    for filej in &files {
        let endpoint = Endpoint {
            parent_url: dir.api_url(),
            key: None,
        };

        let guess = mime_guess::from_path(filej);

        let file = FileMeta::new(
            Some(filej.to_string()),
            None,
            dir.join(filej),
            endpoint,
            download_url.clone(),
            None,
            vec![checksum.clone()],
            guess.first(),
            Some(version.clone()),
            Some(publication_date.clone()),
            None,
            license != "restricted",
        );

        entries.push(Entry::File(file));
    }

    Ok(entries)
}

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

        let entries = analyse_json(&resp, &dir)?;

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
