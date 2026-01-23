use std::{collections::HashMap, str::FromStr, sync::Arc};

use exn::{Exn, OptionExt, ResultExt};
use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION, USER_AGENT},
    ClientBuilder,
};
use serde_json::Value as JsonValue;
use url::Url;

use crate::{
    RepositoryRecord, json_extract, repo::RepositoryExt, repo_impl::{Arxiv, DataDryad, Dataone, DataverseDataset, DataverseFile, GitHub, HalScience, OSF, Zenodo}
};

use std::collections::HashSet;
use std::sync::LazyLock;

#[derive(Debug)]
pub struct DispatchError {
    pub message: String,
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for DispatchError {}

static DATAONE_DOMAINS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    HashSet::from([
        "arcticdata.io",
        "knb.ecoinformatics.org",
        "data.pndb.fr",
        "opc.dataone.org",
        "portal.edirepository.org",
        "goa.nceas.ucsb.edu",
        "data.piscoweb.org",
        "adc.arm.gov",
        "scidb.cn",
        "data.ess-dive.lbl.gov",
        "hydroshare.org",
        "ecl.earthchem.org",
        "get.iedadata.org",
        "usap-dc.org",
        "iys.hakai.org",
        "doi.pangaea.de",
        "rvdata.us",
        "sead-published.ncsa.illinois.edu",
    ])
});

static DATAVERSE_DOMAINS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    HashSet::from([
        "www.march.es",
        "www.murray.harvard.edu",
        "abacus.library.ubc.ca",
        "ada.edu.au",
        "adattar.unideb.hu",
        "archive.data.jhu.edu",
        "borealisdata.ca",
        "dados.ipb.pt",
        "dadosdepesquisa.fiocruz.br",
        "darus.uni-stuttgart.de",
        "data.aussda.at",
        "data.cimmyt.org",
        "data.fz-juelich.de",
        "data.goettingen-research-online.de",
        "data.inrae.fr",
        "data.scielo.org",
        "data.sciencespo.fr",
        "data.tdl.org",
        "data.univ-gustave-eiffel.fr",
        "datarepositorium.uminho.pt",
        "datasets.iisg.amsterdam",
        "dataspace.ust.hk",
        "dataverse.asu.edu",
        "dataverse.cirad.fr",
        "dataverse.csuc.cat",
        "dataverse.harvard.edu",
        "dataverse.iit.it",
        "dataverse.ird.fr",
        "dataverse.lib.umanitoba.ca",
        "dataverse.lib.unb.ca",
        "dataverse.lib.virginia.edu",
        "dataverse.nl",
        "dataverse.no",
        "dataverse.openforestdata.pl",
        "dataverse.scholarsportal.info",
        "dataverse.theacss.org",
        "dataverse.ucla.edu",
        "dataverse.unc.edu",
        "dataverse.unimi.it",
        "dataverse.yale-nus.edu.sg",
        "dorel.univ-lorraine.fr",
        "dvn.fudan.edu.cn",
        "edatos.consorciomadrono.es",
        "edmond.mpdl.mpg.de",
        "heidata.uni-heidelberg.de",
        "lida.dataverse.lt",
        "mxrdr.icm.edu.pl",
        "osnadata.ub.uni-osnabrueck.de",
        "planetary-data-portal.org",
        "qdr.syr.edu",
        "rdm.aau.edu.et",
        "rdr.kuleuven.be",
        "rds.icm.edu.pl",
        "recherche.data.gouv.fr",
        "redu.unicamp.br",
        "repod.icm.edu.pl",
        "repositoriopesquisas.ibict.br",
        "research-data.urosario.edu.co",
        "researchdata.cuhk.edu.hk",
        "researchdata.ntu.edu.sg",
        "rin.lipi.go.id",
        "ssri.is",
        "www.seanoe.org",
        "trolling.uit.no",
        "www.sodha.be",
        "www.uni-hildesheim.de",
        "dataverse.acg.maine.edu",
        "dataverse.icrisat.org",
        "datos.pucp.edu.pe",
        "datos.uchile.cl",
        "opendata.pku.edu.cn",
    ])
});

// get default branch's commit
// NOTE: this might reach rate limit as well, therefore need a client as parameter.
async fn github_get_default_branch_commit(
    owner: &str,
    repo: &str,
) -> Result<String, Exn<DispatchError>> {
    // TODO: don't panic, and wrap client.get as client.get_json() to be used everywhere.
    let user_agent = format!("datahugger-cli/{}", env!("CARGO_PKG_VERSION"));
    let mut headers = HeaderMap::new();
    if let Ok(token) = std::env::var("GITHUB_TOKEN") {
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("token {token}")).unwrap(),
        );
    }
    headers.insert(USER_AGENT, HeaderValue::from_str(&user_agent).unwrap());
    let client = ClientBuilder::new()
        .user_agent(&user_agent)
        .default_headers(headers)
        .use_native_tls()
        .build()
        .unwrap();
    let repo_url = format!("https://api.github.com/repos/{owner}/{repo}");
    let resp: JsonValue = client
        .get(&repo_url)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let default_branch: String =
        json_extract(&resp, "default_branch").map_err(|_| DispatchError {
            message: "not able to get default branch".to_string(),
        })?;

    let commits_url =
        format!("https://api.github.com/repos/{owner}/{repo}/commits/{default_branch}");

    let resp: JsonValue = client
        .get(&commits_url)
        .header("User-Agent", user_agent.clone())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let commit_sha: String = json_extract(&resp, "sha").map_err(|_| DispatchError {
        message: "not able to get default branch".to_string(),
    })?;

    Ok(commit_sha)
}

/// # Errors
/// ???
#[allow(clippy::too_many_lines)]
pub async fn resolve(url: &str) -> Result<RepositoryRecord, Exn<DispatchError>> {
    let url = Url::from_str(url).or_raise(|| DispatchError {
        message: format!("'{url}' not a valid url"),
    })?;
    let scheme = url.scheme();
    let domain = url.domain().ok_or_else(|| DispatchError {
        message: "domain unresolved".to_string(),
    })?;
    let host_str = url.host_str().ok_or_else(|| DispatchError {
        message: format!("host_str unresolved from '{url}'"),
    })?;

    // DataOne spec hosted
    if DATAONE_DOMAINS.contains(domain) {
        // https://data.ess-dive.lbl.gov/view/doi%3A10.15485%2F1971251
        // resolved to xml at https://cn.dataone.org/cn/v2/object/doi%3A10.15485%2F1971251
        let mut segments = url.path_segments().ok_or_else(|| DispatchError {
            message: format!("'{url}' cannot be base"),
        })?;
        let id = segments
            .find(|pat| pat.starts_with("doi"))
            .ok_or_raise(|| DispatchError {
                message: format!("expect 'doi' in '{url}'"),
            })?;

        let base_url = format!("{scheme}://{host_str}");
        let base_url = Url::from_str(&base_url).or_raise(|| DispatchError {
            message: format!("'{base_url}' is not valid url"),
        })?;
        let repo = Arc::new(Dataone::new(base_url));
        let record = repo.get_record(id);
        return Ok(record);
    }

    // Dataverse spec hosted
    if DATAVERSE_DOMAINS.contains(domain) {
        // https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD
        // https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/KBHLOD/JCJCJC
        let mut segments = url.path_segments().ok_or_else(|| DispatchError {
            message: format!("'{url}' cannot be base"),
        })?;
        let typ = segments.next().ok_or_else(|| DispatchError {
            message: format!("'{url}' no segments found"),
        })?;
        let queries = url.query_pairs();
        let queries = queries.collect::<HashMap<_, _>>();
        let Some(id) = queries.get("persistentId") else {
            exn::bail!(DispatchError {
                message: "query don't contains 'persistentId'".to_string()
            })
        };

        let typ = typ.strip_suffix(".xhtml").ok_or_else(|| DispatchError {
            message: "segment not in format *.xhtml".to_string(),
        })?;
        let base_url = format!("{scheme}://{host_str}");
        let base_url = Url::from_str(&base_url).or_raise(|| DispatchError {
            message: format!("'{base_url}' is not valid url"),
        })?;
        let version = ":latest-published".to_string();
        match typ {
            "dataset" => {
                let repo = Arc::new(DataverseDataset::new(base_url, version));
                let record = repo.get_record(id);
                return Ok(record);
            }
            "file" => {
                let repo = Arc::new(DataverseFile::new(base_url, version));
                let record = repo.get_record(id);
                return Ok(record);
            }
            ty => exn::bail!(DispatchError {
                message: format!("{ty} is not valid type, can only be 'dataset' or 'file'")
            }),
        }
    }

    match domain {
        "arxiv.org" => {
            let mut segments = url.path_segments().ok_or_else(|| DispatchError {
                message: format!("cannot get path segments of url '{}'", url.as_str()),
            })?;
            let id = segments
                .next()
                .and_then(|_| segments.next())
                .ok_or(DispatchError {
                    message: format!("connot get record id from '{url}'"),
                })?;

            let repo = Arc::new(Arxiv::new());
            let record = repo.get_record(id);
            Ok(record)
        }
        "hal.science" => {
            let mut segments = url.path_segments().ok_or_else(|| DispatchError {
                message: format!("cannot get path segments of url '{}'", url.as_str()),
            })?;
            let id = segments
                .next()
                .ok_or(DispatchError {
                    message: format!("connot get record id from '{url}'"),
                })?;

            let repo = Arc::new(HalScience::new());
            let record = repo.get_record(id);
            Ok(record)
        }
        "huggingface.co" => todo!(),
        "zenodo.org" => {
            let segments = url
                .path_segments()
                .ok_or_else(|| DispatchError {
                    message: format!("cannot get path segments of url '{}'", url.as_str()),
                })?
                .collect::<Vec<&str>>();
            let record_id = if segments.len() >= 2 {
                segments[1]
            } else {
                exn::bail!(DispatchError {
                    message: format!("unable to parse dryad dataset id from '{url}'",)
                })
            };
            let repo = Arc::new(Zenodo::new());
            let record = repo.get_record(record_id);
            Ok(record)
        }
        "github.com" => {
            let mut segments = url.path_segments().ok_or_else(|| DispatchError {
                message: format!("cannot get path segments of url '{}'", url.as_str()),
            })?;

            let owner = segments.next().ok_or_else(|| DispatchError {
                message: format!("missing owner in url '{}'", url.as_str()),
            })?;

            let repo_name = segments.next().ok_or_else(|| DispatchError {
                message: format!("missing repo in url '{}'", url.as_str()),
            })?;

            let record = if let Some(id) = segments.next().and_then(|_| segments.next()) {
                let repo = Arc::new(GitHub::new(owner, repo_name));
                repo.get_record(id)
            } else {
                let id = github_get_default_branch_commit(owner, repo_name).await?;
                let repo = Arc::new(GitHub::new(owner, repo_name));
                repo.get_record(&id)
            };

            Ok(record)
        }
        "datadryad.org" => {
            // example url: https://datadryad.org/dataset/doi:10.5061/dryad.mj8m0
            let segments = url
                .path_segments()
                .ok_or_else(|| DispatchError {
                    message: format!("cannot get path segments of url '{}'", url.as_str()),
                })?
                .collect::<Vec<&str>>();
            // id is 'doi:10.5061/dryad.mj8m0'
            let record_id = if segments.len() >= 3 && segments[0] == "dataset" {
                format!("{}/{}", segments[1], segments[2])
            } else {
                exn::bail!(DispatchError {
                    message: format!("unable to parse dryad dataset id from '{url}'",)
                })
            };
            let base_url = Url::from_str("https://datadryad.org/").or_raise(|| DispatchError {
                message: "invalid base url".to_string(),
            })?;

            let repo = Arc::new(DataDryad::new(base_url));
            let record = repo.get_record(&record_id);
            Ok(record)
        }
        "osf.io" => {
            let mut segments = url.path_segments().ok_or_else(|| DispatchError {
                message: format!("cannot get path segments of url '{}'", url.as_str()),
            })?;

            let id = segments.next().ok_or_else(|| DispatchError {
                message: format!("no segments path in url '{}'", url.as_str()),
            })?;

            let repo = Arc::new(OSF::new());
            let record = repo.get_record(id);
            Ok(record)
        }
        "data.mendeley.com" => todo!(),
        "data.4tu.nl" => todo!(),
        // DataVerse repositories (extracted from re3data)
        "b2share.eudat.eu" | "data.europa.eu" => todo!(),
        _ => {
            exn::bail!(DispatchError {
                message: format!("unknown domain: {domain}")
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_resolve_dataverse_default() {
        // dataset
        let url = "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD";
        let qr = resolve(url).await.unwrap();
        assert_eq!(qr.record_id.as_str(), "doi:10.7910/DVN/KBHLOD");
        qr.repo.as_any().downcast_ref::<DataverseDataset>().unwrap();

        // file
        let url =
            "https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/KBHLOD/DHJ45U";
        let qr = resolve(url).await.unwrap();
        assert_eq!(qr.record_id.as_str(), "doi:10.7910/DVN/KBHLOD/DHJ45U");
        qr.repo.as_any().downcast_ref::<DataverseFile>().unwrap();
    }

    #[tokio::test]
    async fn test_resolve_default() {
        // osf.io
        for url in ["https://osf.io/dezms/overview", "https://osf.io/dezms/"] {
            let qr = resolve(url).await.unwrap();
            assert_eq!(qr.record_id.as_str(), "dezms");
            qr.repo.as_any().downcast_ref::<OSF>().unwrap();
        }
    }
}
