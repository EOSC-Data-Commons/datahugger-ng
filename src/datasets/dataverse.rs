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

fn analyse_json(json: &JsonValue, dir: &DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
    let files = json
        .get("data")
        .and_then(|d| d.get("files"))
        .and_then(JsonValue::as_array)
        .ok_or_else(|| RepoError {
            message: "field with key 'data.files' not resolve to an json array".to_string(),
        })?;

    let mut entries = Vec::with_capacity(files.len());
    for (idx, filej) in files.iter().enumerate() {
        let endpoint = Endpoint {
            parent_url: dir.api_url().clone(),
            key: Some(format!("data.files.{idx}")),
        };
        let name: String = json_extract(filej, "dataFile.filename").or_raise(|| RepoError {
            message: "fail to extracting 'dataFile.filename' as String from json".to_string(),
        })?;
        let restricted: bool = json_extract(filej, "restricted").or_raise(|| RepoError {
            message: "fail to extracting 'dataFile.filename' as String from json".to_string(),
        })?;
        let downloadable = !restricted;
        let id: u64 = json_extract(filej, "dataFile.id").or_raise(|| RepoError {
            message: "fail to extracting 'dataFile.id' as u64 from json".to_string(),
        })?;
        let size: u64 = json_extract(filej, "dataFile.filesize").or_raise(|| RepoError {
            message: "fail to extracting 'dataFile.filesize' as u64 from json".to_string(),
        })?;
        let creation_date: String =
            json_extract(filej, "dataFile.creationDate").or_raise(|| RepoError {
                message: "fail to extracting 'dataFile.creationDate' as String from json"
                    .to_string(),
            })?;
        let last_modification_date: String = json_extract(filej, "dataFile.lastUpdateTime")
            .or_raise(|| RepoError {
                message: "fail to extracting 'dataFile.lastUpdateTime' as String from json"
                    .to_string(),
            })?;
        let mime_type: String =
            json_extract(filej, "dataFile.contentType").or_raise(|| RepoError {
                message: "fail to extracting 'dataFile.contentType' as String from json"
                    .to_string(),
            })?;
        let mime_type = mime::Mime::from_str(&mime_type).or_raise(|| RepoError {
            message: format!("fail to parse the '{}' to proper mime type", mime_type),
        })?;

        let version: u64 = json_extract(filej, "version").or_raise(|| RepoError {
            message: "fail to extracting 'version' as u64 from json".to_string(),
        })?;

        let download_url = dir
            .api_url()
            .join("/api/access/datafile/")
            .or_raise(|| RepoError {
                message: "cannot parse download base url".to_string(),
            })?;
        let download_url = download_url.join(&format!("{id}")).or_raise(|| RepoError {
            message: format!("cannot parse '{download_url}' download url"),
        })?;
        let dst_path = match json_extract::<String>(filej, "directoryLabel") {
            Ok(dir_label) => dir.join(&format!("{dir_label}/{name}")),
            Err(_) => dir.join(&name),
        };
        let checksum_typ: String =
            json_extract(filej, "dataFile.checksum.type").or_raise(|| RepoError {
                message: "fail to extracting 'dataFile.checksum.type' as String from json"
                    .to_string(),
            })?;
        let checksum = match checksum_typ.as_str() {
            "MD5" | "md5" => {
                let hash: String =
                    json_extract(filej, "dataFile.checksum.value").or_raise(|| RepoError {
                        message: "fail to extracting 'dataFile.checksum.value' as String from json"
                            .to_string(),
                    })?;
                Checksum::Md5(hash)
            }
            "SHA-1" | "sha-1" => {
                let hash: String =
                    json_extract(filej, "dataFile.checksum.value").or_raise(|| RepoError {
                        message: "fail to extracting 'dataFile.checksum.value' as String from json"
                            .to_string(),
                    })?;
                Checksum::Sha1(hash)
            }
            v => {
                exn::bail!(RepoError {
                    message: format!(
                        "{v} is not yet support, please open an issue so we can add it"
                    )
                });
            }
        };
        let file = FileMeta::new(
            Some(name),
            Some(id.to_string()),
            dst_path,
            endpoint,
            download_url,
            Some(size),
            vec![checksum],
            Some(mime_type),
            Some(version.to_string()),
            Some(creation_date),
            Some(last_modification_date),
            downloadable,
        );
        entries.push(Entry::File(Box::new(file)));
    }

    Ok(entries)
}

// https://datavers.example/api/datasets/:persistentId/versions/:latest-poblished/?persistentId=<id>
#[derive(Debug)]
pub struct DataverseDataset {
    pub id: String,
    pub base_url: Url,
    pub version: String,
}

impl DataverseDataset {
    #[must_use]
    pub fn new(id: impl Into<String>, base_url: &Url, version: impl Into<String>) -> Self {
        DataverseDataset {
            id: id.into(),
            base_url: base_url.clone(),
            version: version.into(),
        }
    }
}

#[async_trait]
impl DatasetBackend for DataverseDataset {
    fn root_url(&self) -> Url {
        // "https://dataverse.harvard.edu/api/datasets/:persistentId/versions/:latest-published/?persistentId=doi:10.7910/DVN/KBHLOD"
        // Safe to unwrap:
        // - the base URL is a hard-coded, valid absolute URL
        let mut url = self.base_url.clone();
        {
            let mut segments = url.path_segments_mut().unwrap();
            segments.extend([
                "api",
                "datasets",
                ":persistentId",
                "versions",
                &self.version, // e.g. ":latest-published"
            ]);
        }

        url.query_pairs_mut().append_pair("persistentId", &self.id);
        url
    }

    async fn list(&self, client: &Client, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
        println!("{}", dir.api_url());

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

    fn list_from_json(&self, json: &str, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
        let json_value: JsonValue = serde_json::from_str(json).or_raise(|| RepoError {
            message: "Failed to parse JSON".to_string(),
        })?;

        let entries = analyse_json(&json_value, &dir)?;

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// https://datavers.example/api/files/:persistentId/versions/:latest-published/?persistentId=<id>
#[derive(Debug)]
pub struct DataverseFile {
    pub id: String,
    pub base_url: Url,
    pub version: String,
}

impl DataverseFile {
    #[must_use]
    pub fn new(id: &str, base_url: &Url, version: &str) -> Self {
        DataverseFile {
            id: id.to_string(),
            base_url: base_url.clone(),
            version: version.to_string(),
        }
    }
}

#[async_trait]
impl DatasetBackend for DataverseFile {
    fn root_url(&self) -> Url {
        // "https://datavers.example/api/files/:persistentId/versions/:latest-poblished/?persistentId=doi:10.7910/DVN/KBHLOD/DHJ45U"
        // Safe to unwrap:
        // - the base URL is a hard-coded, valid absolute URL
        let mut url = self.base_url.clone();
        {
            let mut segments = url.path_segments_mut().unwrap();
            segments.extend([
                "api",
                "files",
                ":persistentId",
                "versions",
                &self.version, // e.g. ":latest-published"
            ]);
        }

        url.query_pairs_mut().append_pair("persistentId", &self.id);
        url
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

        let filej = resp.get("data").ok_or_else(|| RepoError {
            message: "field with key 'data' not resolve to an json value".to_string(),
        })?;

        let name: String = json_extract(filej, "dataFile.filename").or_raise(|| RepoError {
            message: "fail to extracting 'dataFile.filename' as String from json".to_string(),
        })?;
        let restricted: bool = json_extract(filej, "restricted").or_raise(|| RepoError {
            message: "fail to extracting 'dataFile.filename' as String from json".to_string(),
        })?;
        let downloadable = !restricted;
        let id: u64 = json_extract(filej, "dataFile.id").or_raise(|| RepoError {
            message: "fail to extracting 'dataFile.id' as u64 from json".to_string(),
        })?;

        let size: u64 = json_extract(filej, "dataFile.filesize").or_raise(|| RepoError {
            message: "fail to extracting 'dataFile.filesize' as u64 from json".to_string(),
        })?;
        let mime_type: String =
            json_extract(filej, "dataFile.contentType").or_raise(|| RepoError {
                message: "fail to extracting 'dataFile.contentType' as String from json"
                    .to_string(),
            })?;
        let mime_type = mime::Mime::from_str(&mime_type).or_raise(|| RepoError {
            message: format!("fail to parse the '{}' to proper mime type", mime_type),
        })?;
        let download_url = dir
            .api_url()
            .join("/api/access/datafile/")
            .or_raise(|| RepoError {
                message: "cannot parse download base url".to_string(),
            })?;
        let download_url = download_url.join(&format!("{id}")).or_raise(|| RepoError {
            message: format!("cannot parse '{download_url}' download url"),
        })?;
        let hash: String = json_extract(filej, "dataFile.md5").or_raise(|| RepoError {
            message: "fail to extracting 'dataFile.md5' as String from json".to_string(),
        })?;
        let checksum = Checksum::Md5(hash);
        let endpoint = Endpoint {
            parent_url: dir.api_url().clone(),
            key: Some("data".to_string()),
        };
        let file = FileMeta::new(
            Some(name.clone()),
            Some(id.to_string()),
            dir.join(&name),
            endpoint,
            download_url,
            Some(size),
            vec![checksum],
            Some(mime_type),
            None,
            None,
            None,
            downloadable,
        );
        let entries = vec![Entry::File(Box::new(file))];

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
