use std::{any::Any, fs, path::PathBuf, str::FromStr};

/*
* I do three version for comparison and benchmarking.
* - blocking using std::fs (not using walkdir crate but impl by hand)
* - threadpool async using tokio::fs
* - io_uring.
*/
use crate::{repo::RepoError, CrawlPath, Entry, FileMeta};
use exn::Exn;

pub struct Local {
    root: PathBuf,
}

// FIXME: this should merge with regular DirMeta with seperate root/at out.
#[derive(Debug, Clone)]
pub struct DirMeta {
    path: CrawlPath,
    root: PathBuf,
    at: PathBuf,
}

impl Local {
    fn list(&self, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>> {
        let at_path = dir.at;
        if !at_path.is_dir() {
            // TODO: dontpanic
            panic!("should be a dir")
        }
        let mut entries = Vec::new();
        for entry in fs::read_dir(at_path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                let dir = DirMeta {
                    path: CrawlPath::root().join(&format!("{}", path.display())),
                    root: PathBuf::from_str("/").unwrap(),
                    at: path,
                };
                entries.push(Entry::Dir(dir));
            } else {
                let filename = path.file_name();
                let file = FileMeta::new(
                    None,
                    None,
                    path,
                    endpoint,
                    download_url,
                    None,
                    vec![],
                    None,
                    None,
                    None,
                    None,
                    true,
                );
            }
        }
        Ok(entries)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
