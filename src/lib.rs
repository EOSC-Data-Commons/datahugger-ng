pub mod error;

mod repo;
pub use crate::repo::Checksum;
pub use crate::repo::CrawlPath;
pub use crate::repo::Dataset;
pub use crate::repo::DatasetBackend;
pub use crate::repo::DirMeta;
pub use crate::repo::Entry;
pub use crate::repo::FileMeta;
pub use crate::repo::Hasher;

mod helper;
pub use crate::helper::json_extract;

mod resolver;
pub use crate::resolver::resolve;

pub mod crawler;
pub use crawler::crawl;

mod ops;
pub use crate::ops::{CrawlExt, DownloadExt};

pub mod datasets;
