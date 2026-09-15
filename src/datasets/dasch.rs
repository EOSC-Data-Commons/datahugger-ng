use crate::helper::json_extract;
use crate::repo::{Endpoint, RepoError};
use crate::{Checksum, DatasetBackend, DirMeta, Entry, FileMeta};
use async_trait::async_trait;
use exn::{Exn, ResultExt};
use reqwest_middleware::ClientWithMiddleware;
use serde_json::Value as JsonValue;
use std::any::Any;
use std::str::FromStr;
use url::Url;

fn analyse_json(json: &JsonValue, dir: &DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
    let endpoint = Endpoint {
        parent_url: dir.api_url(),
        key: None, // TODO: figure out how to use this in local data analyzer use case
    };

    let file_id: String = json_extract(json, "fileId").or_raise(|| RepoError {
        message: "fail to extracting 'fileId' as String from json".to_string(),
    })?;

    let file_name: String = json_extract(json, "fileName").or_raise(|| RepoError {
        message: "fail to extracting 'fileName' as String from json".to_string(),
    })?;

    let download_url: String = json_extract(json, "downloadUrl").or_raise(|| RepoError {
        message: "fail to extracting 'downloadUrl' as String from json".to_string(),
    })?;

    let download_url: Url = Url::parse(&download_url).or_raise(|| RepoError {
        message: "fail to parse download url {}".to_string(),
    })?;

    let size: u64 = json_extract(json, "fileSize").or_raise(|| RepoError {
        message: "fail to extracting 'fileSize' as u64 from json".to_string(),
    })?;

    let checksum_typ: String = json_extract(json, "checksumAlgorithm").or_raise(|| RepoError {
        message: "fail to extracting 'checksumAlgorithm' as String from json".to_string(),
    })?;
    let checksum = match checksum_typ.as_str() {
        "MD5" | "md5" => {
            let hash: String = json_extract(json, "checksum").or_raise(|| RepoError {
                message: "fail to extracting 'checksum' as String from json".to_string(),
            })?;
            Checksum::Md5(hash)
        }
        "SHA-256" | "sha-256" => {
            let hash: String = json_extract(json, "checksum").or_raise(|| RepoError {
                message: "fail to extracting 'checksum' as String from json".to_string(),
            })?;
            Checksum::Sha256(hash)
        }
        v => {
            exn::bail!(RepoError {
                message: format!("{v} is not yet supported, please open an issue so we can add it")
            });
        }
    };

    let version: u32 = json_extract(json, "version").or_raise(|| RepoError {
        message: "fail to extracting 'version' as String from json".to_string(),
    })?;

    let creation_date: String = json_extract(json, "dateCreated").or_raise(|| RepoError {
        message: "fail to extracting 'dateCreated' as String from json".to_string(),
    })?;

    let last_modification_date: Option<String> = json_extract(json, "dateModified").ok();

    let file = FileMeta::new(
        Some(file_name.clone()),
        Some(file_id.clone()),
        dir.join(&file_name),
        endpoint,
        download_url,
        Some(size),
        vec![checksum],
        None,
        Some(version.to_string()),
        Some(creation_date),
        last_modification_date,
        true,
    );

    Ok(vec![Entry::File(file)])
}

#[derive(Debug)]
pub struct DaschJsonSrcDataset {
    pub id: String,
    pub content: String,
}

impl DaschJsonSrcDataset {
    #[must_use]
    pub fn new(id: impl Into<String>, content: String) -> Self {
        DaschJsonSrcDataset {
            id: id.into(),
            content,
        }
    }
}

#[async_trait]
impl DatasetBackend for DaschJsonSrcDataset {
    async fn list(
        &self,
        _client: &ClientWithMiddleware,
        dir: DirMeta,
    ) -> Result<Vec<Entry>, Exn<RepoError>> {
        let json_value: JsonValue = serde_json::from_str(&self.content).or_raise(|| RepoError {
            message: "Failed to parse JSON".to_string(),
        })?;

        let entries = analyse_json(&json_value, &dir)?;

        Ok(entries)
    }

    fn root_dir(&self) -> DirMeta {
        let url = Url::from_str("https://repository.dasch.swiss").unwrap();
        DirMeta::new_root(&url)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
