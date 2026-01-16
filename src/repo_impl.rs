#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use url::Url;

use anyhow::anyhow;
use reqwest::Client;
use std::str::FromStr;

use crate::{Checksum, DirMeta, Entry, Repository, json_get, repo::FileMeta};

// https://osf.io/
// API root url at https://api.osf.io/v2/nodes/
#[derive(Debug)]
pub struct OSF {
    client: Client,
}

impl OSF {
    #[must_use]
    pub fn new(client: Client) -> Self {
        OSF { client }
    }
}

#[async_trait]
impl Repository for OSF {
    fn root_url(&self, id: &str) -> Url {
        // https://api.osf.io/v2/nodes/<id>/files to start for every dateset entry

        // Safe to unwrap:
        // - the base URL is a hard-coded, valid absolute URL
        // - `path_segments_mut` cannot fail for this URL scheme
        let mut url = Url::from_str("https://api.osf.io/v2/nodes/").unwrap();
        url.path_segments_mut().unwrap().extend([id, "files"]);
        url
    }

    fn client(&self) -> Client {
        self.client.clone()
    }

    async fn list(&self, dir: DirMeta) -> anyhow::Result<Vec<Entry>> {
        let resp: JsonValue = self
            .client
            .get(dir.api_url.clone())
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let files = resp
            .get("data")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| anyhow!("data not resolve to an array"))?;

        let mut entries = Vec::with_capacity(files.len());
        for filej in files {
            let name: String = json_get(filej, "attributes.name")?;
            let kind: String = json_get(filej, "attributes.kind")?;
            match kind.as_ref() {
                "file" => {
                    let size: u64 = json_get(filej, "attributes.size")?;
                    let download_url: String = json_get(filej, "links.download")?;
                    let download_url = Url::from_str(&download_url)?;
                    let hash: String = json_get(filej, "attributes.extra.hashes.sha256")?;
                    let checksum = Checksum::Sha256(hash);
                    let file =
                        FileMeta::new(dir.join(&name), download_url, Some(size), vec![checksum]);
                    entries.push(Entry::File(file));
                }
                "folder" => {
                    let api_url: String =
                        json_get(filej, "relationships.files.links.related.href")?;
                    let api_url = Url::from_str(&api_url)?;
                    let dir = DirMeta::new(api_url, dir.join(&name));
                    entries.push(Entry::Dir(dir));
                }
                _ => Err(anyhow::anyhow!("kind is not 'file' or 'folder'"))?,
            }
        }

        Ok(entries)
    }
}

// https://datavers.example/api/datasets/:persistentId/versions/:latest-poblished/?persistentId=<id>
#[derive(Debug)]
pub struct DataverseDataset {
    client: Client,
    base_url: Url,
    version: String,
}

impl DataverseDataset {
    #[must_use]
    pub fn new(client: Client, base_url: Url, version: String) -> Self {
        DataverseDataset {
            client,
            base_url,
            version,
        }
    }
}

#[async_trait]
impl Repository for DataverseDataset {
    fn root_url(&self, id: &str) -> Url {
        // "https://datavers.example/api/datasets/:persistentId/versions/:latest-poblished/?persistentId=doi:10.7910/DVN/KBHLOD"
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

        url.query_pairs_mut().append_pair("persistentId", id);
        url
    }

    fn client(&self) -> Client {
        self.client.clone()
    }

    async fn list(&self, dir: DirMeta) -> anyhow::Result<Vec<Entry>> {
        let resp: JsonValue = self
            .client
            .get(dir.api_url.clone())
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await?
            .json()
            .await?;

        let files = resp
            .get("data")
            .and_then(|d| d.get("files"))
            .and_then(JsonValue::as_array)
            .ok_or_else(|| anyhow!("data not resolve to an array"))?;

        let mut entries = Vec::with_capacity(files.len());
        for filej in files {
            let name: String = json_get(filej, "dataFile.filename")?;
            let id: u64 = json_get(filej, "dataFile.id")?;

            let size: u64 = json_get(filej, "dataFile.filesize")?;
            let download_url = Url::from_str("https://dataverse.harvard.edu/api/access/datafile/")
                .expect("a valid url");
            let download_url = download_url.join(&format!("{id}"))?;
            // XXX: Is dataverse only MD5 support? there is dataFile.checksum.value as well
            let hash: String = json_get(filej, "dataFile.md5")?;
            let checksum = Checksum::Md5(hash);
            let file = FileMeta::new(dir.join(&name), download_url, Some(size), vec![checksum]);
            entries.push(Entry::File(file));
        }

        Ok(entries)
    }
}

// https://datavers.example/api/files/:persistentId/versions/:latest-published/?persistentId=<id>
#[derive(Debug)]
pub struct DataverseFile {
    client: Client,
    base_url: Url,
    version: String,
}

impl DataverseFile {
    #[must_use]
    pub fn new(client: Client, base_url: Url, version: String) -> Self {
        DataverseFile {
            client,
            base_url,
            version,
        }
    }
}

#[async_trait]
impl Repository for DataverseFile {
    fn root_url(&self, id: &str) -> Url {
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

        url.query_pairs_mut().append_pair("persistentId", id);
        url
    }

    fn client(&self) -> Client {
        self.client.clone()
    }

    async fn list(&self, dir: DirMeta) -> anyhow::Result<Vec<Entry>> {
        let resp: JsonValue = self
            .client
            .get(dir.api_url.clone())
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await?
            .json()
            .await?;

        let filej = resp
            .get("data")
            .ok_or_else(|| anyhow!("data not resolved"))?;

        let name: String = json_get(filej, "dataFile.filename")?;
        let id: u64 = json_get(filej, "dataFile.id")?;

        let size: u64 = json_get(filej, "dataFile.filesize")?;
        let download_url = Url::from_str("https://dataverse.harvard.edu/api/access/datafile/")
            .expect("a valid url");
        let download_url = download_url.join(&format!("{id}"))?;
        // XXX: Is dataverse only MD5 support? there is dataFile.checksum.value as well
        let hash: String = json_get(filej, "dataFile.md5")?;
        let checksum = Checksum::Md5(hash);
        let file = FileMeta::new(dir.join(&name), download_url, Some(size), vec![checksum]);
        let entries = vec![Entry::File(file)];

        Ok(entries)
    }
}
