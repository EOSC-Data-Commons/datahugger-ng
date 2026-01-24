#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use exn::{Exn, OptionExt, ResultExt};
use url::Url;

use reqwest::{Client, StatusCode};
use std::{any::Any, io::Cursor, str::FromStr};

use crate::{
    repo::{Endpoint, FileMeta, RepoError},
    DatasetBackend, DirMeta, Entry,
};

// https://www.dataone.org/
// API doc at https://dataoneorg.github.io/api-documentation/
// XXX: read about https://dataoneorg.github.io/api-documentation/design/DataPackage.html?utm_source=chatgpt.com
// not planned because Dataone is extremly slow in HTTP response.
// XXX: potentially it support: https://dataoneorg.github.io/api-documentation/apis/MN_APIs.html#MNPackage.getPackage
#[derive(Debug)]
pub struct Dataone {
    pub base_url: Url,
    pub id: String,
}

impl Dataone {
    #[must_use]
    pub fn new(base_url: &Url, id: impl Into<String>) -> Self {
        Dataone {
            base_url: base_url.clone(),
            id: id.into(),
        }
    }
}

#[async_trait]
impl DatasetBackend for Dataone {
    fn root_url(&self) -> Url {
        // the dashboard can be at https://data.ess-dive.lbl.gov/view/doi%3A10.15485%2F1971251
        // the xml to describe datasets are all at https://cn.dataone.org/cn/v2/object/

        // Safe to unwrap:
        // - the base URL is a hard-coded, valid absolute URL
        // - `join` cannot fail for this URL scheme
        let url = Url::from_str("https://cn.dataone.org/cn/v2/object/").unwrap();
        url.join(&self.id).expect("cannot parse new url")
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
