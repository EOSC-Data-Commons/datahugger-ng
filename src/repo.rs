use async_trait::async_trait;
use exn::{Exn, ResultExt};
use futures_core::stream::BoxStream;
use reqwest::Client;
use url::Url;

use async_stream::try_stream;
use std::{any::Any, path::Path, sync::Arc};

use digest::Digest;

use crate::error::ErrorStatus;

const ROOT: &str = "__ROOT__";

/// A logical crawl path used to track the current location during repository crawling.
///
/// `CrawlPath` is a lightweight, owned wrapper around `String` that represents
/// URL-like, slash-separated paths. It intentionally does **not** use filesystem
/// semantics (`PathBuf`), as crawl paths follow logical repository structure rather
/// than OS-specific path rules.
///
/// Paths may be *absolute* (prefixed with a special root marker) or *relative*.
/// The root marker is an internal invariant and is stripped when converting to a
/// relative path.
///
/// This type is always owned to make it safe and ergonomic to use across asynchronous
/// tasks and threads.
///
/// # Invariants
///
/// ```
/// const ROOT: &str = "__ROOT__";
/// ```
///
/// - Absolute paths always start with `ROOT`.
/// - Relative paths never start with `ROOT`.
/// - Path separators are forward slashes (`'/'`).
///
/// # Examples
///
/// ```
/// use datahugger::CrawlPath;
///
/// let root = CrawlPath::root();
/// let p = root.join("dir").join("file.txt");
///
/// assert!(p.is_absolute());
/// assert_eq!(p.relative().as_ref(), std::path::Path::new("dir/file.txt"));
/// ```
#[derive(Debug, Clone)]
pub struct CrawlPath(String);

impl std::fmt::Display for CrawlPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<Path> for CrawlPath {
    fn as_ref(&self) -> &Path {
        Path::new(&self.0)
    }
}

impl CrawlPath {
    /// Appends a path segment to this crawl path, returning a new `CrawlPath`.
    ///
    /// The segment is joined using a forward slash (`'/'`). This method does not
    /// perform normalization and assumes `p` does not contain leading slashes.
    #[must_use]
    pub fn join(&self, p: &str) -> CrawlPath {
        let mut new_path = self.0.clone();
        if !new_path.ends_with('/') {
            new_path.push('/');
        }
        new_path.push_str(p);
        CrawlPath(new_path)
    }

    /// Returns the root crawl path.
    ///
    /// The root path is represented internally using a special marker and is
    /// considered absolute.
    #[must_use]
    pub fn root() -> CrawlPath {
        CrawlPath(ROOT.to_string())
    }

    /// Returns `true` if this path is absolute (i.e. starts from the crawl root).
    #[must_use]
    pub fn is_absolute(&self) -> bool {
        self.0.starts_with(ROOT)
    }

    /// Converts this path into a relative crawl path.
    ///
    /// If the path is absolute, the root marker (and an optional following slash)
    /// is stripped. If the path is already relative, it is returned unchanged.
    ///
    /// An absolute root path (`ROOT` or `ROOT/`) is converted into an empty
    /// relative path.
    ///
    /// # Panics
    ///
    /// Panics if this path is marked as absolute but does not start with `ROOT`.
    /// It indicates a violation of the internal `CrawlPath` invariants.
    #[must_use]
    pub fn relative(&self) -> CrawlPath {
        if !self.is_absolute() {
            return self.clone();
        }

        let rest = self
            .0
            .strip_prefix(ROOT)
            .expect("absolute paths start with ROOT");

        let rest = rest.strip_prefix('/').unwrap_or(rest);

        CrawlPath(rest.to_string())
    }
}

pub enum Hasher {
    Md5(md5::Md5),
    Sha256(sha2::Sha256),
}

impl Hasher {
    pub fn update(&mut self, data: &[u8]) {
        match self {
            Hasher::Md5(h) => h.update(data),
            Hasher::Sha256(h) => h.update(data),
        }
    }

    #[must_use]
    pub fn finalize(self) -> Vec<u8> {
        match self {
            Hasher::Md5(h) => h.finalize().to_vec(),
            Hasher::Sha256(h) => h.finalize().to_vec(),
        }
    }
}

#[derive(Debug)]
pub enum Entry {
    Dir(DirMeta),
    File(FileMeta),
}

#[derive(Debug, Clone)]
pub struct DirMeta {
    path: CrawlPath,
    pub api_url: Url,
}

impl std::fmt::Display for DirMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DirMeta (at: {}, src: {})",
            self.path,
            self.api_url.as_str()
        )
    }
}

impl DirMeta {
    #[must_use]
    pub fn new(api_url: Url, path: CrawlPath) -> Self {
        DirMeta { path, api_url }
    }
    #[must_use]
    pub fn new_root(api_url: Url) -> Self {
        DirMeta {
            path: CrawlPath(ROOT.to_string()),
            api_url,
        }
    }

    #[must_use]
    pub fn relative(&self) -> CrawlPath {
        self.path.relative()
    }

    #[must_use]
    pub fn join(&self, p: &str) -> CrawlPath {
        self.path.join(p)
    }
}

#[derive(Debug)]
pub struct FileMeta {
    path: CrawlPath,
    pub download_url: Url,
    pub size: Option<u64>,
    pub checksum: Vec<Checksum>,
}

impl FileMeta {
    pub fn new(
        path: CrawlPath,
        download_url: Url,
        size: Option<u64>,
        checksum: Vec<Checksum>,
    ) -> Self {
        FileMeta {
            path,
            download_url,
            size,
            checksum,
        }
    }
    pub fn relative(&self) -> CrawlPath {
        self.path.relative()
    }
}

#[derive(Debug)]
pub enum Checksum {
    Md5(String),
    Sha256(String),
}

#[derive(Debug)]
pub struct RepoError {
    pub message: String,
}

impl std::fmt::Display for RepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "repo fail: {}", self.message)
    }
}

impl std::error::Error for RepoError {}

#[derive(Debug)]
pub struct CrawlerError {
    pub message: String,
    pub status: ErrorStatus,
}

impl std::fmt::Display for CrawlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "crawler fail: {}", self.message)
    }
}

impl std::error::Error for CrawlerError {}

#[async_trait]
pub trait Repository: Send + Sync + Any {
    async fn list(&self, client: &Client, dir: DirMeta) -> Result<Vec<Entry>, Exn<RepoError>>;
    fn root_url(&self, id: &str) -> Url;
    fn as_any(&self) -> &dyn Any;
}

pub fn crawl<R>(
    client: Client,
    repo: Arc<R>,
    dir: DirMeta,
) -> BoxStream<'static, Result<Entry, Exn<CrawlerError>>>
where
    R: Repository + 'static + ?Sized,
{
    Box::pin(try_stream! {
        // TODO: this is at boundary need to deal with error to retry.
        let entries = repo.list(&client, dir.clone())
            .await
            .or_raise(||
                CrawlerError{
                    message: format!("cannot list all entries of '{dir}', after retry"),
                    status: ErrorStatus::Persistent,
                })?;

        for entry in entries {
            match entry {
                Entry::File(f) => yield Entry::File(f),
                Entry::Dir(sub_dir) => {
                    yield Entry::Dir(sub_dir.clone());
                    let client = client.clone();
                    let sub_stream = crawl(client, Arc::clone(&repo), sub_dir);
                    for await item in sub_stream {
                        yield item?;
                    }
                }
            }
        }
    })
}
