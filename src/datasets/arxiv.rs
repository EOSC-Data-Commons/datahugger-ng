#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use exn::Exn;
use url::Url;

use reqwest::Client;
use std::{any::Any, str::FromStr};

use crate::{
    repo::{Endpoint, FileMeta, RepoError},
    DatasetBackend, DirMeta, Entry,
};

// https://arxiv.org/
// API root url at https://arxiv.org/pdf/
#[derive(Debug)]
pub struct Arxiv {
    pub id: String,
}

impl Arxiv {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Arxiv { id: id.into() }
    }
}

#[async_trait]
impl DatasetBackend for Arxiv {
    fn root_url(&self) -> Url {
        // https://arxiv.org/pdf/<id> to get the record pdf

        // Safe to unwrap:
        // - the base URL is a hard-coded, valid absolute URL
        // - `path_segments_mut` cannot fail for this URL scheme
        let mut url = Url::from_str("https://arxiv.org").unwrap();
        url.path_segments_mut().unwrap().extend(["pdf", &self.id]);
        url
    }

    async fn list(&self, _client: &Client, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
        let root_url = dir.root_url();
        // safe to unwrap, because I create the root_url
        let name: Vec<&str> = root_url.path_segments().unwrap().collect::<Vec<_>>();
        let name = name[1];
        let download_url = root_url.clone();
        let endpoint = Endpoint {
            parent_url: dir.root_url(),
            key: Some(name.to_string()),
        };
        let file = FileMeta::new(
            dir.join(&format!("{name}.pdf")),
            endpoint,
            download_url,
            None,
            vec![],
        );

        Ok(vec![Entry::File(file)])
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
