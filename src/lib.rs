pub mod error;

mod repo;
pub use crate::repo::Checksum;
pub use crate::repo::CrawlPath;
pub use crate::repo::Dataset;
pub use crate::repo::DatasetBackend;
pub use crate::repo::DirMeta;
pub use crate::repo::Entry;
pub use crate::repo::FileInZipMeta;
pub use crate::repo::FileMeta;
pub use crate::repo::Hasher;
pub use crate::repo::ZipMeta;

mod filter;
pub use crate::filter::FileFilter;

mod helper;

mod resolver;
pub use crate::resolver::dabar_dataset_resolve;
pub use crate::resolver::resolve;
pub use crate::resolver::resolve_doi_to_url;

pub mod crawler;
pub use crawler::crawl;

mod ops;
pub use crate::ops::{CrawlExt, DownloadExt, EntryDump, FileInZipDump, InspectDump};

pub mod datasets;
