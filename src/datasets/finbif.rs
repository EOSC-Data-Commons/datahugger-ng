//! FinBIF (Finnish Biodiversity Information Facility) Darwin Core Archives.
//!
//! A dataset like `https://tun.fi/HR.727` is published as a Darwin Core Archive
//! (a zip) at `https://gbif.laji.fi/archive/HR.727`. The server does not support
//! HTTP range requests, but it exposes the files inside the archive through
//! `https://gbif.laji.fi/metadata/HR.727?file=<name>`. We read `meta.xml` this
//! way to list the archive content without downloading the zip.

use crate::repo::{Endpoint, FileInZipMeta, ZipMeta};
use crate::{repo::RepoError, DatasetBackend, DirMeta, Entry};
use async_trait::async_trait;
use exn::{Exn, ResultExt};
use reqwest::StatusCode;
use reqwest_middleware::ClientWithMiddleware;
use std::any::Any;
use std::path::Path;
use std::str::FromStr;
use url::Url;

const ARCHIVE_BASE: &str = "https://gbif.laji.fi/archive/";
const METADATA_BASE: &str = "https://gbif.laji.fi/metadata/";
const META_FILE: &str = "meta.xml";
const DEFAULT_EML_FILE: &str = "eml.xml";

fn make_file(dir: &DirMeta, name: &str) -> FileInZipMeta {
    let filename = Path::new(name)
        .file_name()
        .and_then(|f| f.to_str())
        .map_or_else(|| name.to_string(), ToString::to_string);
    FileInZipMeta::new(
        Some(filename),
        None,
        dir.join(name),
        mime_guess::from_path(name).first(),
    )
}

/// Build the overview of a Darwin Core Archive from the content of its `meta.xml`.
///
/// The returned entry lists `meta.xml`, the EML file (whose name is declared in the
/// `metadata` attribute of `<archive>`, default `eml.xml`), and every data file
/// referenced by `<core>` and `<extension>` elements.
fn analyse_meta_xml(xml: &str, id: &str, dir: &DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
    let doc = roxmltree::Document::parse(xml).or_raise(|| RepoError {
        message: "failed to parse meta.xml of Darwin Core Archive".to_string(),
    })?;

    let root = doc.root_element();
    if root.tag_name().name() != "archive" {
        exn::bail!(RepoError {
            message: format!(
                "expected root element 'archive' in meta.xml, got '{}'",
                root.tag_name().name()
            ),
        });
    }

    let eml_name = root.attribute("metadata").unwrap_or(DEFAULT_EML_FILE);

    // meta.xml and eml.xml are always part of the archive, followed by the data files
    let mut names: Vec<String> = vec![META_FILE.to_string(), eml_name.to_string()];

    for table in root
        .children()
        .filter(|n| matches!(n.tag_name().name(), "core" | "extension"))
    {
        for location in table
            .descendants()
            .filter(|n| n.tag_name().name() == "location")
        {
            if let Some(text) = location.text().map(str::trim).filter(|t| !t.is_empty()) {
                if !names.iter().any(|n| n == text) {
                    names.push(text.to_string());
                }
            }
        }
    }

    let files: Vec<FileInZipMeta> = names.iter().map(|name| make_file(dir, name)).collect();

    let download_url = Url::from_str(&format!("{ARCHIVE_BASE}{id}")).or_raise(|| RepoError {
        message: format!("invalid archive download url for id '{id}'"),
    })?;

    let endpoint = Endpoint {
        parent_url: dir.api_url(),
        key: None,
    };

    // size, checksum, version and dates are not provided by the server
    let meta = ZipMeta::new(
        endpoint,
        download_url,
        None,
        vec![],
        None,
        None,
        None,
        true,
        files,
    );

    Ok(vec![Entry::Zip(meta)])
}

#[derive(Debug)]
pub struct FinBif {
    pub id: String,
}

impl FinBif {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        FinBif { id: id.into() }
    }
}

#[async_trait]
impl DatasetBackend for FinBif {
    fn root_dir(&self) -> DirMeta {
        let url = Url::from_str(&format!("{METADATA_BASE}{}", self.id)).unwrap();
        DirMeta::new_root(&url)
    }

    async fn list(
        &self,
        client: &ClientWithMiddleware,
        dir: DirMeta,
    ) -> Result<Vec<Entry>, Exn<RepoError>> {
        let mut meta_url = dir.api_url();
        meta_url.query_pairs_mut().append_pair("file", META_FILE);

        let resp = client
            .get(meta_url.clone())
            .send()
            .await
            .or_raise(|| RepoError {
                message: format!("fail at client sent GET {meta_url}"),
            })?;
        let resp = resp.error_for_status().map_err(|err| match err.status() {
            Some(StatusCode::NOT_FOUND) => RepoError {
                message: format!("resource not found when GET {meta_url}"),
            },
            Some(status_code) => RepoError {
                message: format!("fail GET {meta_url}, with state code: {status_code}"),
            },
            None => RepoError {
                message: format!("fail GET {meta_url}, network / protocol error"),
            },
        })?;

        let xml = resp.text().await.or_raise(|| RepoError {
            message: format!("fail GET {meta_url}, unable to read response body"),
        })?;

        analyse_meta_xml(&xml, &self.id, &dir)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const META_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="eml.xml">
<core encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\n" fieldsEnclosedBy="" ignoreHeaderLines="1" rowType="http://rs.tdwg.org/dwc/terms/Occurrence">
<files>
<location>occurrence.txt</location>
</files>
<id index="0"/>
<field index="1" term="http://rs.tdwg.org/dwc/terms/basisOfRecord"/>
</core>
<extension encoding="UTF-8" fieldsTerminatedBy="\t" linesTerminatedBy="\n" fieldsEnclosedBy="" ignoreHeaderLines="1" rowType="http://rs.gbif.org/terms/1.0/Multimedia">
<files>
<location>media.txt</location>
</files>
<coreid index="0"/>
<field index="1" term="http://purl.org/dc/terms/type"/>
</extension>
</archive>"#;

    #[test]
    fn test_analyse_meta_xml() {
        let backend = FinBif::new("HR.85");
        let dir = backend.root_dir();
        assert_eq!(
            dir.api_url().as_str(),
            "https://gbif.laji.fi/metadata/HR.85"
        );

        let entries = analyse_meta_xml(META_XML, "HR.85", &dir).unwrap();
        assert_eq!(entries.len(), 1);
        let Entry::Zip(zip) = &entries[0] else {
            panic!("expected a zip entry");
        };

        assert_eq!(
            zip.download_url().as_str(),
            "https://gbif.laji.fi/archive/HR.85"
        );
        assert!(zip.is_downloadable());
        assert!(zip.checksum().is_empty());
        assert!(zip.size().is_none());

        let names: Vec<String> = zip
            .files()
            .iter()
            .map(|f| f.filename().unwrap().to_string())
            .collect();
        assert_eq!(names, ["meta.xml", "eml.xml", "occurrence.txt", "media.txt"]);
    }

    #[test]
    fn test_analyse_meta_xml_custom_eml_name() {
        let xml = r#"<archive xmlns="http://rs.tdwg.org/dwc/text/" metadata="custom_eml.xml">
<core rowType="http://rs.tdwg.org/dwc/terms/Taxon"><files><location>taxon.txt</location></files><id index="0"/></core>
</archive>"#;
        let dir = FinBif::new("HR.1").root_dir();
        let entries = analyse_meta_xml(xml, "HR.1", &dir).unwrap();
        let Entry::Zip(zip) = &entries[0] else {
            panic!("expected a zip entry");
        };
        let names: Vec<String> = zip
            .files()
            .iter()
            .map(|f| f.filename().unwrap().to_string())
            .collect();
        assert_eq!(names, ["meta.xml", "custom_eml.xml", "taxon.txt"]);
    }

    #[test]
    fn test_analyse_meta_xml_rejects_non_archive() {
        let dir = FinBif::new("HR.1").root_dir();
        assert!(analyse_meta_xml("<foo/>", "HR.1", &dir).is_err());
    }
}
