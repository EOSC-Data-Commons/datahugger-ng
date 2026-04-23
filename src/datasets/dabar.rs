#![allow(clippy::upper_case_acronyms)]

use async_trait::async_trait;
use exn::{Exn, ResultExt};
use url::Url;

use crate::{
    repo::{Endpoint, FileMeta, RepoError},
    DatasetBackend, DirMeta, Entry,
};
use mime::Mime;
use reqwest::Client;
use std::any::Any;

// Namespace constants mirroring Python's NS dict
const NS_MODS: &str = "http://www.loc.gov/mods/v3";

fn make_file_entry(
    file_meta: &roxmltree::Node,
    record_identifier: &roxmltree::Node,
    location: &str,
    dir: &DirMeta,
) -> Result<Entry, Exn<RepoError>> {
    let file_identifier = file_meta.attribute("ID").ok_or_else(|| {
        Exn::new(RepoError {
            message: "file_meta element missing ID attribute".to_string(),
        })
    })?;

    // mods:physicalDescription/mods:internetMediaType
    let mime_text = file_meta
        .descendants()
        .find(|n| {
            n.tag_name().name() == "internetMediaType" && n.tag_name().namespace() == Some(NS_MODS)
        })
        .and_then(|n| n.text())
        .ok_or_else(|| {
            Exn::new(RepoError {
                message: format!("No mimetype found for file_meta ID={file_identifier}"),
            })
        })?;

    let mime: Mime = mime_text.parse::<Mime>().or_raise(|| RepoError {
        message: format!("Invalid mimetype: {mime_text}"),
    })?;

    let size: Option<u64> = file_meta
        .descendants()
        .find(|n| n.tag_name().name() == "extent" && n.tag_name().namespace() == Some(NS_MODS))
        .and_then(|n| n.text())
        .and_then(|s| s.parse::<u64>().ok());

    let record_id_text = record_identifier.text().ok_or_else(|| {
        Exn::new(RepoError {
            message: "record identifier has no text content".to_string(),
        })
    })?;

    let download_url: Url = format!("{location}/{file_identifier}/download")
        .parse::<Url>()
        .or_raise(|| RepoError {
            message: format!("Could not build download URL for identifier={record_id_text}"),
        })?;

    let endpoint = Endpoint {
        parent_url: dir.api_url(),
        key: None, // TODO: figure out how to use this in local data analyzer use case
    };

    Ok(Entry::File(FileMeta::new(
        None,
        Some(file_identifier.to_string()),
        dir.join(""), // adjust to your CrawlPath construction
        endpoint,     // adjust to your Endpoint construction
        download_url,
        size,
        vec![],
        Some(mime),
        None,
        None,
        None,
        true,
    )))
}

fn analyze_xml(
    doc: &roxmltree::Document,
    dir: &DirMeta,
    location: &str,
) -> Result<Vec<Entry>, Exn<RepoError>> {
    let root = doc.root_element();

    // /oai:record/oai:metadata//mods:identifier[@type="local"]
    let record_identifier = root.descendants().find(|n| {
        n.tag_name().name() == "identifier"
            && n.tag_name().namespace() == Some(NS_MODS)
            && n.attribute("type") == Some("local")
    });

    // /oai:record/oai:metadata//mods:location/mods:url
    let record_url = root
        .descendants()
        .find(|n| n.tag_name().name() == "url" && n.tag_name().namespace() == Some(NS_MODS));

    // Early return `
    if record_identifier.is_none() || record_url.is_none() {
        return Ok(vec![]);
    }

    let record_identifier = record_identifier.unwrap();

    // /oai:record/oai:metadata//mods:mods[mods:physicalDescription/mods:internetMediaType]
    let entries: Vec<_> = root
        .descendants()
        .filter(|n| {
            n.tag_name().name() == "mods"
                && n.tag_name().namespace() == Some(NS_MODS)
                && n.attribute("ID") != Some("master")
                && n.descendants().any(|child| {
                    child.tag_name().name() == "internetMediaType"
                        && child.tag_name().namespace() == Some(NS_MODS)
                })
        })
        .map(|file_meta| make_file_entry(&file_meta, &record_identifier, location, dir))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(entries)
}

#[derive(Debug)]
pub struct DabarXmlSrcDataset {
    pub id: String,
    pub content: String,
}

impl DabarXmlSrcDataset {
    #[must_use]
    pub fn new(id: impl Into<String>, content: String) -> Self {
        DabarXmlSrcDataset {
            id: id.into(),
            content,
        }
    }
}

#[async_trait]
impl DatasetBackend for DabarXmlSrcDataset {
    fn root_url(&self) -> Url {
        Url::parse("https://dabar.srce.hr/oai/").unwrap() // static OAI-PMH repo URL
    }

    async fn list(&self, _client: &Client, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
        let doc = roxmltree::Document::parse(&self.content).or_raise(|| RepoError {
            message: "Failed to parse XML".to_string(),
        })?;

        // /oai:record/oai:metadata//mods:location/mods:url[@displayLabel="URN:NBN"]
        let urn_url_node = doc
            .descendants()
            .find(|n| {
                n.tag_name().name() == "url"
                    && n.tag_name().namespace() == Some(NS_MODS)
                    && n.attribute("displayLabel") == Some("URN:NBN")
            })
            .ok_or_else(|| {
                Exn::new(RepoError {
                    message: "No location url (URN:NBN) found in record".to_string(),
                })
            })?;

        let urn_url_text = urn_url_node.text().ok_or_else(|| {
            Exn::new(RepoError {
                message: "URN:NBN url node has no text".to_string(),
            })
        })?;

        // Build a client to resolve the URL handle to figure out the domain to build the download links.
        // TODO: Once DABAR MODS contains the domains, this logic can be removed.
        let no_redirect_client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .or_raise(|| RepoError {
                message: "Failed to build HTTP client".to_string(),
            })?;

        let response = no_redirect_client
            .head(urn_url_text)
            .send()
            .await
            .or_raise(|| RepoError {
                message: format!("HEAD request failed for {urn_url_text}"),
            })?;

        let location = response
            .headers()
            .get("Location")
            .ok_or_else(|| {
                Exn::new(RepoError {
                    message: "No location header in response".to_string(),
                })
            })?
            .to_str()
            .or_raise(|| RepoError {
                message: "Location header is not valid UTF-8".to_string(),
            })?;

        let entries = analyze_xml(&doc, &dir, location)?;

        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CrawlPath;

    #[tokio::test]
    async fn test_list() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<record xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <header>
    <identifier>oai:dabar.srce.hr:agr_2814</identifier>
    <datestamp>2025-10-27</datestamp>
  </header>
  <metadata>
    <modsCollection xmlns="http://www.loc.gov/mods/v3" xmlns:dabar="http://dabar.srce.hr/standards/schema/1.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:etd="http://www.ndltd.org/standards/metadata/etdms/1.0" xmlns:datacite="http://datacite.org/schema/kernel-4" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-8.xsd http://dabar.srce.hr/standards/schema/1.0 https://dabar.srce.hr/standards/schema/1.0/dabar.xsd">
      <mods ID="master" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-6.xsd">
        <identifier type="local">agr:2814</identifier>
        <name type="personal">
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/aut">author</roleTerm>
          </role>
          <namePart type="given">Lana</namePart>
          <namePart type="family">Filipović</namePart>
        </name>
        <name type="personal">
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/aut">author</roleTerm>
          </role>
          <namePart type="given">Vilim</namePart>
          <namePart type="family">Filipović</namePart>
        </name>
        <name type="personal">
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/aut">author</roleTerm>
          </role>
          <namePart type="given">Zoran</namePart>
          <namePart type="family">Kovač</namePart>
        </name>
        <name type="personal">
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/aut">author</roleTerm>
          </role>
          <namePart type="given">Vedran</namePart>
          <namePart type="family">Krevh</namePart>
        </name>
        <name type="personal">
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/aut">author</roleTerm>
          </role>
          <namePart type="given">Jasmina</namePart>
          <namePart type="family">Defterdarović</namePart>
        </name>
        <titleInfo lang="eng" usage="primary">
          <title>SUPREHILL Critical Zone Observatory dataset - funded by Croatian Science Foundation (HRZZ)</title>
        </titleInfo>
        <language>
          <languageTerm type="code" authority="iso639-2b">eng</languageTerm>
        </language>
        <genre authority="HRZVO-KR-HRZVO-KR-Vrsta_podataka" lang="hrv" valueURI="HRZVO-KR-HRZVO-KR-Vrsta_podataka:3">eksperimentalni podaci</genre>
        <genre authority="HRZVO-KR-HRZVO-KR-Vrsta_podataka" lang="eng" valueURI="HRZVO-KR-HRZVO-KR-Vrsta_podataka:3">experimental data</genre>
        <genre authority="coar" authorityURI="https://vocabularies.coar-repositories.org/resource_types/" valueURI="http://purl.org/coar/resource_type/63NG-B465">experimental data</genre>
        <abstract lang="eng" type="primary">Data collected at the SUPREHILL Critical Zone Observatory (CZO), funded by Croatian Science Foundation (HRZZ)</abstract>
        <subject lang="eng" usage="primary">
          <topic>SUPREHILL</topic>
          <topic>critical zone observatory</topic>
          <topic>vadose zone</topic>
          <topic>hillslope</topic>
          <topic>vineyard</topic>
        </subject>
        <subject authority="nvzz.hr" ID="4#4.01#4.01.03">
          <topic lang="hrv">Biotehničke znanosti</topic>
          <topic lang="eng">Biotechnical Sciences</topic>
          <topic lang="hrv">Poljoprivreda</topic>
          <topic lang="eng">Agriculture</topic>
          <topic lang="hrv">ekologija i zaštita okoliša</topic>
          <topic lang="eng">Ecology and Environmental Protection</topic>
        </subject>
        <relatedItem type="constituent" displayLabel="project">
          <identifier type="local">4284</identifier>
          <identifier>UIP-2019-04-5409</identifier>
          <titleInfo lang="hrv">
            <title>Podpovršinski preferencijalni transportni procesi u poljoprivrednim padinskim tlima</title>
          </titleInfo>
          <titleInfo lang="eng">
            <title>SUbsurface PREferential transport processes in agricultural HILLslope soils</title>
          </titleInfo>
          <name type="personal">
            <role>
              <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/pdr">project director</roleTerm>
              <roleTerm lang="hrv" type="text">Voditelj projekta</roleTerm>
            </role>
            <namePart>Vilim Filipović</namePart>
          </name>
          <name type="corporate" authority="iso3166">
            <role>
              <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/jug">jurisdiction governed</roleTerm>
            </role>
            <namePart>Hrvatska</namePart>
          </name>
          <name type="corporate">
            <role>
              <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/fnd">funder</roleTerm>
            </role>
            <namePart displayLabel="funder name">Hrvatska zaklada za znanost</namePart>
          </name>
          <note type="funding" displayLabel="funder programme">Installation Research Projects</note>
          <titleInfo type="abbreviated">
            <title>SUPREHILL</title>
          </titleInfo>
        </relatedItem>
        <accessCondition type="restriction on access" authority="HRZVO-KR-PravaPristupa">openAccess</accessCondition>
        <accessCondition type="use and reproduction">http://rightsstatements.org/vocab/InC/1.0/</accessCondition>
        <physicalDescription/>
        <physicalDescription/>
        <physicalDescription/>
        <subject>
          <geographic authority="iso3166">HR</geographic>
          <geographic>Jazbina</geographic>
        </subject>
        <abstract type="methods" lang="eng">Data collected at the SUPREHILL CZO (https://sites.google.com/view/suprehill) is separated into three main categories.: 1) data collected by field measurements 2) data collected by individual field and laboratory experiments 3) data collected by laboratory analyses</abstract>
        <name type="corporate">
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/pbl">publisher</roleTerm>
            <roleTerm type="text" lang="hrv">izdavač</roleTerm>
          </role>
          <namePart lang="hrv">Agronomski fakultet</namePart>
          <namePart lang="eng">Faculty of Agriculture</namePart>
        </name>
        <location>
          <url access="object in context" usage="primary" displayLabel="URN:NBN">https://urn.nsk.hr/urn:nbn:hr:204:468943</url>
        </location>
        <identifier type="urn">urn:nbn:hr:204:468943</identifier>
        <recordInfo>
          <recordIdentifier>agr:2814/mods:2023-02-22T12:55:29+01:00</recordIdentifier>
          <recordCreationDate encoding="iso8601">2023-02-22T12:55:29+01:00</recordCreationDate>
          <recordContentSource authority="local">agr</recordContentSource>
          <recordContentSource>Repozitorij Agronomskog fakulteta u Zagrebu</recordContentSource>
          <recordChangeDate encoding="iso8601">2024-02-27T13:54:10+01:00</recordChangeDate>
        </recordInfo>
        <name type="personal">
          <namePart type="given">Lana</namePart>
          <namePart type="family">Filipović</namePart>
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/dtc">data contributor</roleTerm>
            <roleTerm type="text" lang="hrv">Djelatnik</roleTerm>
          </role>
        </name>
        <name type="personal">
          <namePart type="given">Valentina</namePart>
          <namePart type="family">Bezek</namePart>
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/edt">editor</roleTerm>
            <roleTerm type="text">data editor</roleTerm>
          </role>
        </name>
        <genre authority="dabar" type="object type">dataset</genre>
        <extension>
          <dabar:kontaktZaCjelovitiTekst>lfilipovic@agr.hr</dabar:kontaktZaCjelovitiTekst>
        </extension>
      </mods>
      <mods ID="FILE0" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-6.xsd">
        <physicalDescription>
          <internetMediaType>application/zip</internetMediaType>
        </physicalDescription>
        <abstract displayLabel="data description" lang="eng">SUPREHILL database</abstract>
        <accessCondition type="restriction on access" authority="HRZVO-KR-PravaPristupa">openAccess</accessCondition>
        <accessCondition type="use and reproduction">http://rightsstatements.org/vocab/InC/1.0/</accessCondition>
        <identifier type="file">primary file</identifier>
      </mods>
      <mods ID="DOC0" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-6.xsd">
        <physicalDescription>
          <internetMediaType>application/vnd.openxmlformats-officedocument.wordprocessingml.document</internetMediaType>
        </physicalDescription>
        <accessCondition type="restriction on access" authority="HRZVO-KR-PravaPristupa">openAccess</accessCondition>
        <accessCondition type="use and reproduction">http://rightsstatements.org/vocab/InC/1.0/</accessCondition>
      </mods>
    </modsCollection>
  </metadata>
</record>"#;

        let dataset = DabarXmlSrcDataset::new("test-id", xml.to_string());
        let client = Client::new();
        let dir = DirMeta::new(
            CrawlPath::root(),
            Url::parse("https://example.com/api").unwrap(),
            Url::parse("https://example.com").unwrap(),
        );

        let entries = dataset.list(&client, dir).await.unwrap();

        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_analyze_xml() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<record xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <header>
    <identifier>oai:dabar.srce.hr:agr_2814</identifier>
    <datestamp>2025-10-27</datestamp>
  </header>
  <metadata>
    <modsCollection xmlns="http://www.loc.gov/mods/v3" xmlns:dabar="http://dabar.srce.hr/standards/schema/1.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:etd="http://www.ndltd.org/standards/metadata/etdms/1.0" xmlns:datacite="http://datacite.org/schema/kernel-4" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-8.xsd http://dabar.srce.hr/standards/schema/1.0 https://dabar.srce.hr/standards/schema/1.0/dabar.xsd">
      <mods ID="master" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-6.xsd">
        <identifier type="local">agr:2814</identifier>
        <name type="personal">
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/aut">author</roleTerm>
          </role>
          <namePart type="given">Lana</namePart>
          <namePart type="family">Filipović</namePart>
        </name>
        <name type="personal">
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/aut">author</roleTerm>
          </role>
          <namePart type="given">Vilim</namePart>
          <namePart type="family">Filipović</namePart>
        </name>
        <name type="personal">
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/aut">author</roleTerm>
          </role>
          <namePart type="given">Zoran</namePart>
          <namePart type="family">Kovač</namePart>
        </name>
        <name type="personal">
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/aut">author</roleTerm>
          </role>
          <namePart type="given">Vedran</namePart>
          <namePart type="family">Krevh</namePart>
        </name>
        <name type="personal">
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/aut">author</roleTerm>
          </role>
          <namePart type="given">Jasmina</namePart>
          <namePart type="family">Defterdarović</namePart>
        </name>
        <titleInfo lang="eng" usage="primary">
          <title>SUPREHILL Critical Zone Observatory dataset - funded by Croatian Science Foundation (HRZZ)</title>
        </titleInfo>
        <language>
          <languageTerm type="code" authority="iso639-2b">eng</languageTerm>
        </language>
        <genre authority="HRZVO-KR-HRZVO-KR-Vrsta_podataka" lang="hrv" valueURI="HRZVO-KR-HRZVO-KR-Vrsta_podataka:3">eksperimentalni podaci</genre>
        <genre authority="HRZVO-KR-HRZVO-KR-Vrsta_podataka" lang="eng" valueURI="HRZVO-KR-HRZVO-KR-Vrsta_podataka:3">experimental data</genre>
        <genre authority="coar" authorityURI="https://vocabularies.coar-repositories.org/resource_types/" valueURI="http://purl.org/coar/resource_type/63NG-B465">experimental data</genre>
        <abstract lang="eng" type="primary">Data collected at the SUPREHILL Critical Zone Observatory (CZO), funded by Croatian Science Foundation (HRZZ)</abstract>
        <subject lang="eng" usage="primary">
          <topic>SUPREHILL</topic>
          <topic>critical zone observatory</topic>
          <topic>vadose zone</topic>
          <topic>hillslope</topic>
          <topic>vineyard</topic>
        </subject>
        <subject authority="nvzz.hr" ID="4#4.01#4.01.03">
          <topic lang="hrv">Biotehničke znanosti</topic>
          <topic lang="eng">Biotechnical Sciences</topic>
          <topic lang="hrv">Poljoprivreda</topic>
          <topic lang="eng">Agriculture</topic>
          <topic lang="hrv">ekologija i zaštita okoliša</topic>
          <topic lang="eng">Ecology and Environmental Protection</topic>
        </subject>
        <relatedItem type="constituent" displayLabel="project">
          <identifier type="local">4284</identifier>
          <identifier>UIP-2019-04-5409</identifier>
          <titleInfo lang="hrv">
            <title>Podpovršinski preferencijalni transportni procesi u poljoprivrednim padinskim tlima</title>
          </titleInfo>
          <titleInfo lang="eng">
            <title>SUbsurface PREferential transport processes in agricultural HILLslope soils</title>
          </titleInfo>
          <name type="personal">
            <role>
              <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/pdr">project director</roleTerm>
              <roleTerm lang="hrv" type="text">Voditelj projekta</roleTerm>
            </role>
            <namePart>Vilim Filipović</namePart>
          </name>
          <name type="corporate" authority="iso3166">
            <role>
              <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/jug">jurisdiction governed</roleTerm>
            </role>
            <namePart>Hrvatska</namePart>
          </name>
          <name type="corporate">
            <role>
              <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/fnd">funder</roleTerm>
            </role>
            <namePart displayLabel="funder name">Hrvatska zaklada za znanost</namePart>
          </name>
          <note type="funding" displayLabel="funder programme">Installation Research Projects</note>
          <titleInfo type="abbreviated">
            <title>SUPREHILL</title>
          </titleInfo>
        </relatedItem>
        <accessCondition type="restriction on access" authority="HRZVO-KR-PravaPristupa">openAccess</accessCondition>
        <accessCondition type="use and reproduction">http://rightsstatements.org/vocab/InC/1.0/</accessCondition>
        <physicalDescription/>
        <physicalDescription/>
        <physicalDescription/>
        <subject>
          <geographic authority="iso3166">HR</geographic>
          <geographic>Jazbina</geographic>
        </subject>
        <abstract type="methods" lang="eng">Data collected at the SUPREHILL CZO (https://sites.google.com/view/suprehill) is separated into three main categories.: 1) data collected by field measurements 2) data collected by individual field and laboratory experiments 3) data collected by laboratory analyses</abstract>
        <name type="corporate">
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/pbl">publisher</roleTerm>
            <roleTerm type="text" lang="hrv">izdavač</roleTerm>
          </role>
          <namePart lang="hrv">Agronomski fakultet</namePart>
          <namePart lang="eng">Faculty of Agriculture</namePart>
        </name>
        <location>
          <url access="object in context" usage="primary" displayLabel="URN:NBN">https://urn.nsk.hr/urn:nbn:hr:204:468943</url>
        </location>
        <identifier type="urn">urn:nbn:hr:204:468943</identifier>
        <recordInfo>
          <recordIdentifier>agr:2814/mods:2023-02-22T12:55:29+01:00</recordIdentifier>
          <recordCreationDate encoding="iso8601">2023-02-22T12:55:29+01:00</recordCreationDate>
          <recordContentSource authority="local">agr</recordContentSource>
          <recordContentSource>Repozitorij Agronomskog fakulteta u Zagrebu</recordContentSource>
          <recordChangeDate encoding="iso8601">2024-02-27T13:54:10+01:00</recordChangeDate>
        </recordInfo>
        <name type="personal">
          <namePart type="given">Lana</namePart>
          <namePart type="family">Filipović</namePart>
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/dtc">data contributor</roleTerm>
            <roleTerm type="text" lang="hrv">Djelatnik</roleTerm>
          </role>
        </name>
        <name type="personal">
          <namePart type="given">Valentina</namePart>
          <namePart type="family">Bezek</namePart>
          <role>
            <roleTerm type="text" authority="loc" authorityURI="https://id.loc.gov/vocabulary/relators" valueURI="http://id.loc.gov/vocabulary/relators/edt">editor</roleTerm>
            <roleTerm type="text">data editor</roleTerm>
          </role>
        </name>
        <genre authority="dabar" type="object type">dataset</genre>
        <extension>
          <dabar:kontaktZaCjelovitiTekst>lfilipovic@agr.hr</dabar:kontaktZaCjelovitiTekst>
        </extension>
      </mods>
      <mods ID="FILE0" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-6.xsd">
        <physicalDescription>
          <internetMediaType>application/zip</internetMediaType>
        </physicalDescription>
        <abstract displayLabel="data description" lang="eng">SUPREHILL database</abstract>
        <accessCondition type="restriction on access" authority="HRZVO-KR-PravaPristupa">openAccess</accessCondition>
        <accessCondition type="use and reproduction">http://rightsstatements.org/vocab/InC/1.0/</accessCondition>
        <identifier type="file">primary file</identifier>
      </mods>
      <mods ID="DOC0" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-6.xsd">
        <physicalDescription>
          <internetMediaType>application/vnd.openxmlformats-officedocument.wordprocessingml.document</internetMediaType>
        </physicalDescription>
        <accessCondition type="restriction on access" authority="HRZVO-KR-PravaPristupa">openAccess</accessCondition>
        <accessCondition type="use and reproduction">http://rightsstatements.org/vocab/InC/1.0/</accessCondition>
      </mods>
    </modsCollection>
  </metadata>
</record>
"#;

        let doc = roxmltree::Document::parse(xml).unwrap();
        let dir = DirMeta::new(
            CrawlPath::root(),
            Url::parse("https://example.com/api").unwrap(),
            Url::parse("https://example.com").unwrap(),
        );

        let location = "https://repozitorij.agr.unizg.hr/object/agr:2814";

        let entries = analyze_xml(&doc, &dir, location).unwrap();

        println!("{:?}", entries);

        assert_eq!(entries.len(), 2);
    }
}
