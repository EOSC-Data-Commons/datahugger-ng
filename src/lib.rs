mod repo;
pub use crate::repo::Checksum;
pub use crate::repo::CrawlPath;
pub use crate::repo::DirMeta;
pub use crate::repo::Entry;
pub use crate::repo::Hasher;
pub use crate::repo::Repository;
pub use crate::repo::crawl;

pub mod repo_impl;

mod helper;
pub use crate::helper::json_extract;

mod download;
pub use crate::download::download_with_validation;

mod dispatch;
pub use crate::dispatch::{QueryRepository, resolve};

pub mod error;
