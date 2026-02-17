use std::{collections::HashMap, str::FromStr};

use exn::{Exn, OptionExt, ResultExt};
use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION, USER_AGENT},
    ClientBuilder,
};
use serde_json::Value as JsonValue;
use url::Url;

use crate::{
    datasets::{
        Arxiv, DataDryad, Dataone, DataverseDataset, DataverseFile, GitHub, HalScience,
        HuggingFace, Zenodo, OSF,
    },
    json_extract,
    repo::Dataset,
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

#[derive(Debug)]
pub struct ResolveError {
    pub message: String,
}

impl std::fmt::Display for ResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ResolveError {}

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

async fn resolve_doi_to_url_with_base(
    doi: &str,
    base_url: Option<&str>,
) -> Result<String, Exn<ResolveError>> {
    // check if doi is valid
    if !(doi.starts_with("10.") && doi.contains('/')) {
        exn::bail!(ResolveError {
            message: format!("Invalid DOI: '{doi}'"),
        });
    }

    let base_url = base_url.unwrap_or("https://doi.org");

    let client = ClientBuilder::new()
        .use_native_tls()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .or_raise(|| ResolveError {
            message: String::from("Could not build reqwest client"),
        })?;

    let res = match client.get(format!("{}/{}", base_url, doi)).send().await {
        Ok(res) => res,
        Err(err) => {
            exn::bail!(ResolveError {
                message: format!("failed to resolve '{doi}': {err:?}")
            })
        }
    };

    let location = match res.headers().get("Location") {
        Some(header_value) => header_value
            .to_str()
            .or_raise(|| ResolveError {
                message: String::from("Invalid Location header value"),
            })?
            .to_string(),
        None => {
            exn::bail!(ResolveError {
                message: String::from("No Location header in response")
            })
        }
    };

    Ok(location)
}

pub async fn resolve_doi_to_url(doi: &str) -> Result<String, Exn<ResolveError>> {
    resolve_doi_to_url_with_base(doi, None).await
}

/// # Errors
/// ???
#[allow(clippy::too_many_lines)]
pub async fn resolve(url: &str) -> Result<Dataset, Exn<DispatchError>> {
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
        let base_url = format!("{scheme}://{host_str}");
        let base_url = Url::from_str(&base_url).or_raise(|| DispatchError {
            message: format!("'{base_url}' is not valid url"),
        })?;
        let mut segments = url.path_segments().ok_or_else(|| DispatchError {
            message: format!("'{url}' cannot be base"),
        })?;
        let id = segments
            .find(|pat| pat.starts_with("doi"))
            .ok_or_raise(|| DispatchError {
                message: format!("expect 'doi' in '{url}'"),
            })?;

        let dataset = Dataset::new(Dataone::new(&base_url, id));
        return Ok(dataset);
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
                let dataset = Dataset::new(DataverseDataset::new(id.as_ref(), &base_url, &version));
                return Ok(dataset);
            }
            "file" => {
                let dataset = Dataset::new(DataverseFile::new(id.as_ref(), &base_url, &version));
                return Ok(dataset);
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

            let dataset = Dataset::new(Arxiv::new(id));
            Ok(dataset)
        }
        "hal.science" => {
            let mut segments = url.path_segments().ok_or_else(|| DispatchError {
                message: format!("cannot get path segments of url '{}'", url.as_str()),
            })?;
            let id = segments.next().ok_or(DispatchError {
                message: format!("connot get record id from '{url}'"),
            })?;

            let dataset = Dataset::new(HalScience::new(id));
            Ok(dataset)
        }
        "huggingface.co" => {
            eprintln!(
                "\x1b[33mwarning:\x1b[0m for reliable downloads, consider using the official Hugging Face APIs:\n\
                 \x1b[36m  - Rust:\x1b[0m hf_hub\n\
                 \x1b[36m  - Python:\x1b[0m datasets\n\
                 \n\
                 \x1b[2mFor example, datahugger would handle caching, retries, and consistency for you.\x1b[0m"
            );
            let mut segments = url.path_segments().ok_or_else(|| DispatchError {
                message: format!("cannot get path segments of url '{}'", url.as_str()),
            })?;

            let kind = segments.next().ok_or_else(|| DispatchError {
                message: format!("missing repo kind in url '{}'", url.as_str()),
            })?;

            // Currently only datasets are supported
            if kind != "datasets" {
                exn::bail!(DispatchError {
                    message: format!("unsupported Hugging Face repo kind '{kind}'"),
                });
            }

            let owner = segments.next().ok_or_else(|| DispatchError {
                message: format!("missing owner in url '{}'", url.as_str()),
            })?;

            let repo = segments.next().ok_or_else(|| DispatchError {
                message: format!("missing repo name in url '{}'", url.as_str()),
            })?;

            // URL forms:
            // /datasets/{owner}/{repo}
            // /datasets/{owner}/{repo}/tree/{revision}/...
            let (revision, _subpath) = match segments.next() {
                Some("tree") => {
                    let rev = segments.next().ok_or_else(|| DispatchError {
                        message: format!("missing revision in url '{}'", url.as_str()),
                    })?;
                    let rest: Vec<&str> = segments.collect();
                    (rev, rest.join("/"))
                }
                _ => ("main", String::new()),
            };

            let dataset = Dataset::new(HuggingFace::new(owner, repo, revision));
            Ok(dataset)
        }
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
            let dataset = Dataset::new(Zenodo::new(record_id));
            Ok(dataset)
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

            let dataset = if let Some(branch_or_commit) =
                segments.next().and_then(|_| segments.next())
            {
                Dataset::new(GitHub::new(owner, repo_name, branch_or_commit))
            } else {
                let branch_or_commit = github_get_default_branch_commit(owner, repo_name).await?;
                Dataset::new(GitHub::new(owner, repo_name, branch_or_commit))
            };

            Ok(dataset)
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

            let dataset = Dataset::new(DataDryad::new(record_id, &base_url));
            Ok(dataset)
        }
        "osf.io" => {
            let mut segments = url.path_segments().ok_or_else(|| DispatchError {
                message: format!("cannot get path segments of url '{}'", url.as_str()),
            })?;

            let id = segments.next().ok_or_else(|| DispatchError {
                message: format!("no segments path in url '{}'", url.as_str()),
            })?;

            let dataset = Dataset::new(OSF::new(id));
            Ok(dataset)
        }
        "data.mendeley.com" => {
            unimplemented!("help us! open an issue to request or PR to help us.")
        }
        "data.4tu.nl" => {
            unimplemented!("help us! open an issue to request or PR to help us.")
        }
        // DataVerse repositories (extracted from re3data)
        "b2share.eudat.eu" | "data.europa.eu" => {
            unimplemented!("help us! open an issue to request or PR to help us.")
        }
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

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    #[tokio::test]
    async fn test_resolve_dataverse_default() {
        // dataset
        let url = "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD";
        let qr = resolve(url).await.unwrap();
        let qr = qr
            .backend
            .as_any()
            .downcast_ref::<DataverseDataset>()
            .unwrap();
        assert_eq!(qr.id.as_str(), "doi:10.7910/DVN/KBHLOD");

        // file
        let url =
            "https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/KBHLOD/DHJ45U";
        let qr = resolve(url).await.unwrap();
        let qr = qr.backend.as_any().downcast_ref::<DataverseFile>().unwrap();
        assert_eq!(qr.id.as_str(), "doi:10.7910/DVN/KBHLOD/DHJ45U");
    }

    #[tokio::test]
    async fn test_resolve_default() {
        // osf.io
        for url in ["https://osf.io/dezms/overview", "https://osf.io/dezms/"] {
            let qr = resolve(url).await.unwrap();
            let qr = qr.backend.as_any().downcast_ref::<OSF>().unwrap();
            assert_eq!(qr.id.as_str(), "dezms");
        }

        // arxiv
        let url = "https://arxiv.org/abs/2101.00001v1";
        let qr = resolve(url).await.unwrap();
        let qr = qr.backend.as_any().downcast_ref::<Arxiv>().unwrap();
        assert_eq!(qr.id.as_str(), "2101.00001v1");

        // Dataone
        let url = "https://arcticdata.io/catalog/view/doi%3A10.18739%2FA2542JB2X";
        let qr = resolve(url).await.unwrap();
        let qr = qr.backend.as_any().downcast_ref::<Dataone>().unwrap();
        assert_eq!(qr.id.as_str(), "doi%3A10.18739%2FA2542JB2X");
        assert_eq!(qr.base_url.as_str(), "https://arcticdata.io/");

        // dryad
        let url = "https://datadryad.org/dataset/doi:10.5061/dryad.mj8m0";
        let qr = resolve(url).await.unwrap();
        let qr = qr.backend.as_any().downcast_ref::<DataDryad>().unwrap();
        assert_eq!(qr.id.as_str(), "doi:10.5061/dryad.mj8m0");

        // github
        // let url = "https://github.com/EOSC-Data-Commons/datahugger-ng";
        // let qr = resolve(url).await.unwrap();
        // let qr = qr.backend.as_any().downcast_ref::<GitHub>().unwrap();
        // assert_eq!(qr.owner.as_str(), "EOSE-Data-Commons");
        // assert_eq!(qr.repo.as_str(), "datahugger-ng");
        // assert_eq!(
        //     qr.branch_or_commit.as_str(),
        //     "<commit number that can change because by default is the commit of default branch>"
        // );

        // hal
        let url = "https://hal.science/cel-01830944";
        let qr = resolve(url).await.unwrap();
        let qr = qr.backend.as_any().downcast_ref::<HalScience>().unwrap();
        assert_eq!(qr.id.as_str(), "cel-01830944");

        // huggingface
        let url = "https://huggingface.co/datasets/HuggingFaceFW/finepdfs";
        let qr = resolve(url).await.unwrap();
        let qr = qr.backend.as_any().downcast_ref::<HuggingFace>().unwrap();
        assert_eq!(qr.owner.as_str(), "HuggingFaceFW");
        assert_eq!(qr.repo.as_str(), "finepdfs");
        assert_eq!(qr.revision.as_str(), "main");

        // zenodo
        let url = "https://zenodo.org/records/17867222";
        let qr = resolve(url).await.unwrap();
        let qr = qr.backend.as_any().downcast_ref::<Zenodo>().unwrap();
        assert_eq!(qr.id.as_str(), "17867222");
    }

    #[tokio::test]
    async fn test_resolve_doi_to_url() {
        // test valid doi and mock HTTP call
        let mock_server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/10.34894/0B7ZLK"))
            .respond_with(ResponseTemplate::new(301).append_header(
                "Location",
                "https://dataverse.nl/citation?persistentId=doi:10.34894/0B7ZLK",
            ))
            .mount(&mock_server)
            .await;

        let res = resolve_doi_to_url_with_base("10.34894/0B7ZLK", Some(&mock_server.uri())).await;

        assert!(res.is_ok());

        let url = res.unwrap();
        assert_eq!(
            url,
            "https://dataverse.nl/citation?persistentId=doi:10.34894/0B7ZLK"
        );

        // test an invalid DOI
        let res = resolve_doi_to_url_with_base(
            "https://dpoi.org/10.34894/0B7ZLK",
            Some(&mock_server.uri()),
        )
        .await;

        assert!(res.is_err());

        assert_eq!(
            res.unwrap_err().message,
            "Invalid DOI: 'https://dpoi.org/10.34894/0B7ZLK'"
        );
    }
}
