#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use exn::{Exn, OptionExt, ResultExt};
use serde_json::Value as JsonValue;
use url::Url;

use reqwest::{Client, StatusCode};
use std::{any::Any, io::Cursor, str::FromStr};

use crate::{
    json_extract,
    repo::{Endpoint, FileMeta, RepoError},
    Checksum, DirMeta, Entry, Repository,
};

// https://www.dataone.org/
// API doc at https://dataoneorg.github.io/api-documentation/
// TODO: read about https://dataoneorg.github.io/api-documentation/design/DataPackage.html?utm_source=chatgpt.com
// TODO: potentially it support: https://dataoneorg.github.io/api-documentation/apis/MN_APIs.html#MNPackage.getPackage
#[derive(Debug)]
pub struct Dataone {
    #[allow(dead_code)]
    base_url: Url,
}

impl Dataone {
    #[must_use]
    pub fn new(base_url: Url) -> Self {
        Dataone { base_url }
    }
}

#[async_trait]
impl Repository for Dataone {
    fn root_url(&self, id: &str) -> Url {
        // the dashboard can be at https://data.ess-dive.lbl.gov/view/doi%3A10.15485%2F1971251
        // the xml to describe datasets are all at https://cn.dataone.org/cn/v2/object/

        // Safe to unwrap:
        // - the base URL is a hard-coded, valid absolute URL
        // - `join` cannot fail for this URL scheme
        let url = Url::from_str("https://cn.dataone.org/cn/v2/object/").unwrap();
        url.join(id).expect("cannot parse new url")
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
        // TODO: I use xmltree at the moment, which load full xml and then the parsed tree in
        // memory, it can be improve by buf and async when performance requirements comes for
        // DataOne repositories.
        let bytes = resp.bytes().await.map_err(|_| RepoError {
            message: "Failed to get bytes from response".to_string(),
        })?;
        let meta_tree = xmltree::Element::parse(Cursor::new(bytes)).map_err(|_| RepoError {
            message: "Failed to parse XML".to_string(),
        })?;

        let mut entries = Vec::new();
        if let Some(dataset_elem) = meta_tree.get_child("dataset") {
            for data_elem in &dataset_elem.children {
                if let Some(elem) = data_elem.as_element() {
                    if elem.name == "otherEntity" || elem.name == "dataTable" {
                        let download_url = elem
                            .get_child("physical")
                            .and_then(|p| p.get_child("distribution"))
                            .and_then(|d| {
                                d.get_child("online").and_then(|o| {
                                    o.get_child("url").and_then(|url_elem| {
                                        if url_elem
                                            .attributes
                                            .get("function")
                                            .is_some_and(|f| f == "download")
                                        {
                                            url_elem.get_text().map(|s| s.to_string())
                                        } else {
                                            None
                                        }
                                    })
                                })
                            })
                            .ok_or_raise(|| RepoError {
                                message: format!(
                                    "not found download url at {}, through 'physical.distribution.online.url.function.download", 
                                    dir.api_url.as_str()),
                            })?;
                        let download_url = Url::from_str(&download_url).map_err(|_| RepoError {
                            message: format!("{download_url} is not a valid download url"),
                        })?;

                        let name = elem
                            .get_child("entityName")
                            .and_then(|e| e.get_text().map(|s| s.to_string()))
                            .ok_or_raise(|| RepoError {
                                message: "name not found".to_string(),
                            })?;

                        let size = elem
                            .get_child("physical")
                            .and_then(|p| p.get_child("size"))
                            .and_then(|s| {
                                s.get_text().map(|s| {
                                    s.parse::<u64>().map_err(|err| RepoError {
                                        message: format!("cannot parse file physical size, {err}"),
                                    })
                                })
                            })
                            .transpose()?;

                        let endpoint = Endpoint {
                            parent_url: dir.api_url.clone(),
                            key: Some(
                                "dataset.physical.distribution.online.url[@function='download']"
                                    .to_string(),
                            ),
                        };

                        let file =
                            FileMeta::new(dir.join(&name), endpoint, download_url, size, vec![]);
                        entries.push(Entry::File(file));
                    }
                }
            }
        }

        Ok(entries)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

// https://osf.io/
// API root url at https://api.osf.io/v2/nodes/
#[derive(Debug)]
pub struct OSF;

impl OSF {
    #[must_use]
    pub fn new() -> Self {
        OSF
    }
}

impl Default for OSF {
    fn default() -> Self {
        Self::new()
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
            .get("data")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| RepoError {
                message: "field with key 'data' not resolve to an json array".to_string(),
            })?;

        let mut entries = Vec::with_capacity(files.len());
        for (idx, filej) in files.iter().enumerate() {
            let endpoint = Endpoint {
                parent_url: dir.api_url.clone(),
                key: Some(format!("data.{idx}")),
            };
            let name: String = json_extract(filej, "attributes.name").or_raise(|| RepoError {
                message: "fail to extracting 'attributes.name' as String from json".to_string(),
            })?;
            let kind: String = json_extract(filej, "attributes.kind").or_raise(|| RepoError {
                message: "fail to extracting 'attributes.kind' as String from json".to_string(),
            })?;
            match kind.as_ref() {
                "file" => {
                    let size: u64 =
                        json_extract(filej, "attributes.size").or_raise(|| RepoError {
                            message: "fail to extracting 'attributes.size' as u64 from json"
                                .to_string(),
                        })?;
                    let download_url: String =
                        json_extract(filej, "links.download").or_raise(|| RepoError {
                            message: "fail to extracting 'links.download' as String from json"
                                .to_string(),
                        })?;
                    let download_url = Url::from_str(&download_url).or_raise(|| RepoError {
                        message: format!("cannot parse '{download_url}' download url"),
                    })?;
                    let hash: String = json_extract(filej, "attributes.extra.hashes.sha256")
                        .or_raise(|| RepoError {
                            message: "fail to extracting 'attributes.extra.hashes.sha256' as String from json"
                                .to_string(),
                        })?;
                    let checksum = Checksum::Sha256(hash);
                    let file = FileMeta::new(
                        dir.join(&name),
                        endpoint,
                        download_url,
                        Some(size),
                        vec![checksum],
                    );
                    entries.push(Entry::File(file));
                }
                "folder" => {
                    let api_url: String =
                        json_extract(filej, "relationships.files.links.related.href")
                        .or_raise(|| RepoError {
                            message: "fail to extracting 'relationships.files.links.related.href' as String from json"
                                .to_string(),
                        })?;
                    let api_url = Url::from_str(&api_url).or_raise(|| RepoError {
                        message: format!("cannot parse '{api_url}' api url"),
                    })?;
                    let dir = DirMeta::new(api_url, dir.join(&name));
                    entries.push(Entry::Dir(dir));
                }
                typ => {
                    exn::bail!(RepoError {
                        message: format!(
                            "kind can be 'dataset' or 'kind' for an OSF entry, got {typ}"
                        )
                    });
                }
            }
        }

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// https://datavers.example/api/datasets/:persistentId/versions/:latest-poblished/?persistentId=<id>
#[derive(Debug)]
pub struct DataverseDataset {
    base_url: Url,
    version: String,
}

impl DataverseDataset {
    #[must_use]
    pub fn new(base_url: Url, version: String) -> Self {
        DataverseDataset { base_url, version }
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
            .get("data")
            .and_then(|d| d.get("files"))
            .and_then(JsonValue::as_array)
            .ok_or_else(|| RepoError {
                message: "field with key 'data.files' not resolve to an json array".to_string(),
            })?;

        let mut entries = Vec::with_capacity(files.len());
        for (idx, filej) in files.iter().enumerate() {
            let endpoint = Endpoint {
                parent_url: dir.api_url.clone(),
                key: Some(format!("data.files.{idx}")),
            };
            let name: String = json_extract(filej, "dataFile.filename").or_raise(|| RepoError {
                message: "fail to extracting 'dataFile.filename' as String from json".to_string(),
            })?;
            let id: u64 = json_extract(filej, "dataFile.id").or_raise(|| RepoError {
                message: "fail to extracting 'dataFile.id' as u64 from json".to_string(),
            })?;
            let size: u64 = json_extract(filej, "dataFile.filesize").or_raise(|| RepoError {
                message: "fail to extracting 'dataFile.filesize' as u64 from json".to_string(),
            })?;
            let download_url = "https://dataverse.harvard.edu/api/access/datafile/";
            let download_url = Url::from_str(download_url).or_raise(|| RepoError {
                message: format!("cannot parse '{download_url}' download base url"),
            })?;
            let download_url = download_url.join(&format!("{id}")).or_raise(|| RepoError {
                message: format!("cannot parse '{download_url}' download url"),
            })?;
            // XXX: Is dataverse only MD5 support? there is dataFile.checksum.value as well
            let hash: String = json_extract(filej, "dataFile.md5").or_raise(|| RepoError {
                message: "fail to extracting 'dataFile.md5' as String from json".to_string(),
            })?;
            let checksum = Checksum::Md5(hash);
            let file = FileMeta::new(
                dir.join(&name),
                endpoint,
                download_url,
                Some(size),
                vec![checksum],
            );
            entries.push(Entry::File(file));
        }

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// https://datavers.example/api/files/:persistentId/versions/:latest-published/?persistentId=<id>
#[derive(Debug)]
pub struct DataverseFile {
    base_url: Url,
    version: String,
}

impl DataverseFile {
    #[must_use]
    pub fn new(base_url: Url, version: String) -> Self {
        DataverseFile { base_url, version }
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

        let filej = resp.get("data").ok_or_else(|| RepoError {
            message: "field with key 'data' not resolve to an json value".to_string(),
        })?;

        let name: String = json_extract(filej, "dataFile.filename").or_raise(|| RepoError {
            message: "fail to extracting 'dataFile.filename' as String from json".to_string(),
        })?;
        let id: u64 = json_extract(filej, "dataFile.id").or_raise(|| RepoError {
            message: "fail to extracting 'dataFile.id' as u64 from json".to_string(),
        })?;

        let size: u64 = json_extract(filej, "dataFile.filesize").or_raise(|| RepoError {
            message: "fail to extracting 'dataFile.filesize' as u64 from json".to_string(),
        })?;
        let download_url = "https://dataverse.harvard.edu/api/access/datafile/";
        let download_url = Url::from_str(download_url).or_raise(|| RepoError {
            message: format!("cannot parse '{download_url}' download base url"),
        })?;
        let download_url = download_url.join(&format!("{id}")).or_raise(|| RepoError {
            message: format!("cannot parse '{download_url}' download url"),
        })?;
        // XXX: Is dataverse only MD5 support? there is dataFile.checksum.value as well
        let hash: String = json_extract(filej, "dataFile.md5").or_raise(|| RepoError {
            message: "fail to extracting 'dataFile.md5' as String from json".to_string(),
        })?;
        let checksum = Checksum::Md5(hash);
        let endpoint = Endpoint {
            parent_url: dir.api_url.clone(),
            key: Some("data".to_string()),
        };
        let file = FileMeta::new(
            dir.join(&name),
            endpoint,
            download_url,
            Some(size),
            vec![checksum],
        );
        let entries = vec![Entry::File(file)];

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
