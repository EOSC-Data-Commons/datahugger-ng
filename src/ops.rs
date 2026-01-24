use async_trait::async_trait;
use exn::{Exn, ResultExt};
use futures_core::stream::BoxStream;
use futures_util::{StreamExt, TryStreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::sync::Arc;

use reqwest::Client;

use crate::{
    crawl,
    crawler::{CrawlerError, ProgressManager},
    error::ErrorStatus,
    Dataset, Entry,
};

use bytes::Buf;
use digest::Digest;
use std::{fs, path::Path};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tracing::{debug, instrument, warn};

use crate::{Checksum, Hasher};

impl Dataset {
    /// crawling and print the metadata of dirs and files
    /// # Errors
    /// when crawl fails
    pub async fn print_meta(
        &self,
        client: &Client,
        mp: MultiProgress,
    ) -> Result<(), Exn<CrawlerError>> {
        let root_dir = self.root_dir();
        crawl(client.clone(), Arc::clone(&self.backend), root_dir, mp)
            .try_for_each_concurrent(10, |entry| async move {
                match entry {
                    Entry::Dir(dir_meta) => {
                        println!("{dir_meta}");
                    }
                    Entry::File(file_meta) => {
                        println!("{file_meta}");
                    }
                }
                Ok(())
            })
            .await
            .or_raise(|| CrawlerError {
                message: "crawl, download and validation failed".to_string(),
                status: ErrorStatus::Permanent,
            })?;
        Ok(())
    }
}

#[allow(clippy::too_many_lines)]
#[instrument(skip(client, mp))]
async fn download_crawled_file_with_validation<P>(
    client: &Client,
    src: Entry,
    dst: P,
    mp: impl ProgressManager,
) -> Result<(), Exn<CrawlerError>>
where
    P: AsRef<Path> + std::fmt::Debug,
{
    debug!("downloading with validating");
    match src {
        Entry::Dir(dir_meta) => {
            let path = dst.as_ref().join(dir_meta.relative());
            // TODO: create_dir to be more strict on stream order
            fs::create_dir_all(path.as_path()).or_raise(|| CrawlerError {
                message: format!("cannot create dir {}", path.display()),
                status: ErrorStatus::Permanent,
            })?;
            Ok(())
        }
        Entry::File(file_meta) => {
            // prepare stream src
            let pb = mp.insert(0, ProgressBar::new_spinner());
            pb.set_style(
                ProgressStyle::with_template("{spinner:.green} {msg}")
                    .expect("indicatif template error"),
            );
            pb.enable_steady_tick(std::time::Duration::from_millis(100));
            pb.set_message(format!("Connecting... {}", file_meta.download_url.as_str()));
            let resp = client
                .get(file_meta.download_url.clone())
                .send()
                .await
                .or_raise(|| CrawlerError {
                    message: format!("fail to send http GET to {}", file_meta.download_url),
                    status: ErrorStatus::Temporary,
                })?
                .error_for_status()
                .or_raise(|| CrawlerError {
                    message: format!("fail to send http GET to {}", file_meta.download_url),
                    // Temporary??
                    status: ErrorStatus::Temporary,
                })?;
            pb.finish_and_clear();
            let mut stream = resp.bytes_stream();
            // prepare file dst
            let path = dst.as_ref().join(file_meta.relative());
            let mut fh = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path.as_path())
                .await
                .or_raise(|| CrawlerError {
                    message: format!("fail on create file at {}", path.display()),
                    status: ErrorStatus::Permanent,
                })?;

            let checksum = file_meta
                .checksum
                .iter()
                .find(|c| matches!(c, Checksum::Sha256(_)))
                .or_else(|| file_meta.checksum.first());
            let expected_size = file_meta.size;
            let (mut hasher, expected_checksum) = if let Some(checksum) = checksum {
                match checksum {
                    Checksum::Sha256(value) => {
                        (Some(Hasher::Sha256(sha2::Sha256::new())), Some(value))
                    }
                    Checksum::Md5(value) => (Some(Hasher::Md5(md5::Md5::new())), Some(value)),
                }
            } else {
                warn!("unable to find expected checksum to verify");
                (None, None)
            };

            let style = ProgressStyle::with_template(
                "{msg:<60} [{bar:40.cyan/blue}] \
                 {decimal_bytes:>8}/{decimal_total_bytes:>8} \
                 ({decimal_bytes_per_sec:>12}, {eta:>3})",
            )
            .unwrap()
            .progress_chars("=>-");
            let pb = if let Some(expected_size) = expected_size {
                mp.insert_from_back(0, ProgressBar::new(expected_size))
            } else {
                mp.insert_from_back(0, ProgressBar::no_length())
            };
            pb.set_style(style);
            pb.enable_steady_tick(std::time::Duration::from_millis(100));
            pb.set_message(compact_path(file_meta.relative().as_str()));

            let mut got_size = 0;
            while let Some(item) = stream.next().await {
                let mut bytes = item.or_raise(|| CrawlerError {
                    message: "reqwest error stream".to_string(),
                    status: ErrorStatus::Permanent,
                })?;
                let chunk = bytes.chunk();
                if let Some(ref mut hasher) = hasher {
                    hasher.update(chunk);
                }
                let bytes_len = bytes.len() as u64;
                got_size += bytes_len;
                fh.write_all_buf(&mut bytes)
                    .await
                    .or_raise(|| CrawlerError {
                        message: "fail at writing to fs".to_string(),
                        status: ErrorStatus::Permanent,
                    })?;
                pb.inc(bytes_len);
            }

            pb.finish_and_clear();

            if let (Some(expected_size), Some(expected_checksum)) =
                (expected_size, expected_checksum)
            {
                if got_size != expected_size {
                    exn::bail!(CrawlerError {
                        message: format!("size wrong, expect {expected_size}, got {got_size}"),
                        status: ErrorStatus::Permanent
                    })
                }

                let checksum = hex::encode(hasher.expect("hasher is not none").finalize());

                if checksum != *expected_checksum {
                    exn::bail!(CrawlerError {
                        message: format!("size wrong, expect {expected_checksum}, got {checksum}"),
                        status: ErrorStatus::Permanent
                    })
                }
            }
            Ok(())
        }
    }
}

fn compact_path(full_path: &str) -> String {
    let path = Path::new(full_path);

    // Get components
    let mut comps: Vec<String> = path
        .parent() // everything except the file name
        .map(|p| {
            p.components()
                .map(|c| {
                    let s = c.as_os_str().to_string_lossy();
                    if s.is_empty() {
                        String::new()
                    } else {
                        s.chars().next().unwrap().to_string()
                    }
                })
                .collect()
        })
        .unwrap_or_default();

    // Add base file name
    if let Some(file_name) = path.file_name() {
        comps.push(file_name.to_string_lossy().to_string());
    }

    // Join with slashes
    comps.join("/")
}

#[async_trait]
pub trait DownloadExt {
    async fn download_with_validation<P>(
        self,
        client: &Client,
        dst_dir: P,
        mp: impl ProgressManager,
        limit: usize,
    ) -> Result<(), Exn<CrawlerError>>
    where
        P: AsRef<Path> + Sync + Send;
}

#[async_trait]
impl DownloadExt for Dataset {
    /// Downloads all files reachable from a repository root URL into a local directory,
    /// validating both checksum and file size for each downloaded file.
    ///
    /// The repository is crawled recursively starting from its root, and all resolved
    /// files are downloaded concurrently (with a bounded level of parallelism).
    /// Each file is written into `dst_dir` at local fs, preserving its relative path, and is verified
    /// after download to ensure data integrity.
    ///
    /// # Validation
    ///
    /// For every file, this function verifies:
    /// - The downloaded file size matches the expected size.
    /// - The computed checksum matches the checksum provided by the repository metadata.
    ///
    /// A validation failure for any file causes the entire operation to fail.
    ///
    /// # Concurrency
    ///
    /// Downloads are performed concurrently with a fixed upper limit to avoid overwhelming
    /// the network or filesystem.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Dataset crawling fails (e.g. invalid URLs or metadata).
    /// - A file cannot be downloaded due to network or I/O errors.
    /// - The destination directory cannot be created or written to.
    /// - File size or checksum validation fails for any file.
    /// - Any underlying repository or HTTP client operation fails.
    ///
    ///
    /// * `P` is A path-like type specifying the destination directory.
    async fn download_with_validation<P>(
        self,
        client: &Client,
        dst_dir: P,
        mp: impl ProgressManager,
        limit: usize,
    ) -> Result<(), Exn<CrawlerError>>
    where
        P: AsRef<Path> + Sync + Send,
    {
        // TODO: deal with zip differently according to input instruction

        let root_dir = self.root_dir();
        let path = dst_dir.as_ref().join(root_dir.relative());
        fs::create_dir_all(path.as_path()).or_raise(|| CrawlerError {
            message: format!("cannot create dir at '{}'", path.display()),
            status: ErrorStatus::Permanent,
        })?;
        crawl(
            client.clone(),
            Arc::clone(&self.backend),
            root_dir,
            mp.clone(),
        )
        // NOTE: limit set to 0 as default for cli download,
        // should set to 20 for polite crawling for every dataset, it limit the stream consumer rate.
        .try_for_each_concurrent(limit, |entry| {
            let dst_dir = dst_dir.as_ref().to_path_buf();
            let mp = mp.clone();
            async move {
                download_crawled_file_with_validation(client, entry, &dst_dir, mp).await?;
                Ok(())
            }
        })
        .await
        .or_raise(|| CrawlerError {
            message: "crawl, download and validation failed".to_string(),
            status: ErrorStatus::Permanent,
        })?;
        Ok(())
    }
}

pub trait CrawlExt {
    fn crawl(
        self,
        client: &Client,
        mp: impl ProgressManager,
    ) -> BoxStream<'static, Result<Entry, Exn<CrawlerError>>>;
}

impl CrawlExt for Dataset {
    fn crawl(
        self,
        client: &Client,
        mp: impl ProgressManager,
    ) -> BoxStream<'static, Result<Entry, Exn<CrawlerError>>> {
        let root_dir = self.root_dir();
        crawl(
            client.clone(),
            Arc::clone(&self.backend),
            root_dir,
            mp.clone(),
        )
    }
}
