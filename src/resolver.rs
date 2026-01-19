use std::{collections::HashMap, str::FromStr, sync::Arc};

use exn::{Exn, ResultExt};
use url::Url;

use crate::{
    DirMeta, Repository,
    repo_impl::{DataverseDataset, DataverseFile, OSF},
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

#[derive(Clone)]
pub struct RepositoryRecord {
    pub repo: Arc<dyn Repository>,
    pub record_id: String,
}

impl RepositoryRecord {
    #[must_use]
    pub fn root_dir(&self) -> DirMeta {
        DirMeta::new_root(self.repo.root_url(&self.record_id))
    }
}

/// # Errors
/// ???
pub fn resolve(url: &str) -> Result<RepositoryRecord, Exn<DispatchError>> {
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
        todo!()
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
                let repo_query = RepositoryRecord {
                    repo,
                    record_id: id.to_string(),
                };
                return Ok(repo_query);
            }
            "file" => {
                let repo = Arc::new(DataverseFile::new(base_url, version));
                let repo_query = RepositoryRecord {
                    repo,
                    record_id: id.to_string(),
                };
                return Ok(repo_query);
            }
            ty => exn::bail!(DispatchError {
                message: format!("{ty} is not valid type, can only be 'dataset' or 'file'")
            }),
        }
    }

    match domain {
        "arxiv.org" => todo!(),
        "zenodo.org" => todo!(),
        "github.com" => todo!(),
        "datadryad.org" => todo!(),
        "huggingface.co" => todo!(),
        "osf.io" => {
            let mut segments = url.path_segments().ok_or_else(|| DispatchError {
                message: format!("cannot get path segments of url '{}'", url.as_str()),
            })?;

            let id = segments.next().ok_or_else(|| DispatchError {
                message: format!("no segments path in url '{}'", url.as_str()),
            })?;

            let repo = Arc::new(OSF::new());
            let repo_query = RepositoryRecord {
                repo,
                record_id: id.to_string(),
            };
            Ok(repo_query)
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
    #[test]
    fn test_resolve_dataverse_default() {
        // dataset
        let url = "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD";
        let qr = resolve(url).unwrap();
        assert_eq!(qr.record_id.as_str(), "doi:10.7910/DVN/KBHLOD");
        qr.repo.as_any().downcast_ref::<DataverseDataset>().unwrap();

        // file
        let url =
            "https://dataverse.harvard.edu/file.xhtml?persistentId=doi:10.7910/DVN/KBHLOD/DHJ45U";
        let qr = resolve(url).unwrap();
        assert_eq!(qr.record_id.as_str(), "doi:10.7910/DVN/KBHLOD/DHJ45U");
        qr.repo.as_any().downcast_ref::<DataverseFile>().unwrap();
    }

    #[test]
    fn test_resolve_default() {
        // osf.io
        for url in ["https://osf.io/dezms/overview", "https://osf.io/dezms/"] {
            let qr = resolve(url).unwrap();
            assert_eq!(qr.record_id.as_str(), "dezms");
            qr.repo.as_any().downcast_ref::<OSF>().unwrap();
        }
    }
}
