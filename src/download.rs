use bytes::Buf;
use digest::Digest;
use exn::{Exn, ResultExt};
use futures_util::{StreamExt, TryStreamExt};
use reqwest::Client;
use std::{fs, path::Path, sync::Arc};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tracing::{info, instrument};

use crate::{
    Checksum, DirMeta, Entry, Hasher, crawl, dispatch::QueryRepository, error::ErrorStatus,
    repo::CrawlerError,
};

// // must be very efficient, both CPU and RAM usage.
// // [x] need async,
// // [x] need buffer,
// // [x] need reuse HTTP client
// #[instrument(skip(client))]
// async fn download_file<P>(client: &Client, src: FileEntry, dst_root: P) -> anyhow::Result<()>
// where
//     P: AsRef<Path> + std::fmt::Debug,
// {
//     info!("downloading");
//     let resp = client.get(src.link).send().await?.error_for_status()?;
//     let mut stream = resp.bytes_stream();
//
//     // create dst path relative to root
//     let dst = dst_root.as_ref().join(src.rel_path);
//     if src.is_dir {
//         fs::create_dir_all(dst)?;
//         return Ok(());
//     }
//
//     let mut fh = OpenOptions::new()
//         .write(true)
//         .create(true)
//         .truncate(true)
//         .open(dst)
//         .await?;
//     while let Some(item) = stream.next().await {
//         let mut bytes = item?;
//         fh.write_all_buf(&mut bytes).await?;
//     }
//     Ok(())
// }

// /// download files resolved from a url into a folder
// /// # Errors
// /// ???
// pub async fn download<P>(url: &Url, dst_dir: P) -> anyhow::Result<()>
// where
//     P: AsRef<Path>,
// {
//     // TODO: deal with zip differently according to input instruction
//     let client = ClientBuilder::new().build()?;
//
//     crawl(client.clone(), url.clone(), ".")
//         .try_for_each_concurrent(20, |f| {
//             let dst_dir = dst_dir.as_ref().to_path_buf();
//             let client = client.clone();
//             async move {
//                 let mut dst = dst_dir;
//                 dst.push(&f.rel_path);
//                 download_file(&client, f, &dst).await
//             }
//         })
//         .await?;
//     Ok(())
// }

#[instrument(skip(client))]
async fn download_crawled_file_with_validation<P>(
    client: &Client,
    src: Entry,
    dst: P,
) -> Result<(), Exn<CrawlerError>>
where
    P: AsRef<Path> + std::fmt::Debug,
{
    info!("downloading with validating");
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
                .or_else(|| file_meta.checksum.first())
                .ok_or_else(|| CrawlerError {
                    message: "no checksum found on file metadata".to_string(),
                    status: ErrorStatus::Permanent,
                })?;
            let (mut hasher, expected_checksum) = match checksum {
                Checksum::Sha256(value) => (Hasher::Sha256(sha2::Sha256::new()), value),
                Checksum::Md5(value) => (Hasher::Md5(md5::Md5::new()), value),
            };
            let expected_size = file_meta.size.ok_or_else(|| CrawlerError {
                message: "no size found at the file metadata".to_string(),
                status: ErrorStatus::Permanent,
            })?;
            let mut got_size = 0;

            while let Some(item) = stream.next().await {
                let mut bytes = item.or_raise(|| CrawlerError {
                    message: "reqwest error stream".to_string(),
                    status: ErrorStatus::Permanent,
                })?;
                let chunk = bytes.chunk();
                hasher.update(chunk);
                got_size += bytes.len() as u64;
                fh.write_all_buf(&mut bytes)
                    .await
                    .or_raise(|| CrawlerError {
                        message: "fail at writing to fs".to_string(),
                        status: ErrorStatus::Permanent,
                    })?;
            }

            if got_size != expected_size {
                exn::bail!(CrawlerError {
                    message: format!("size wrong, expect {expected_size}, got {got_size}"),
                    status: ErrorStatus::Permanent
                })
            }

            let checksum = hex::encode(hasher.finalize());

            if checksum != *expected_checksum {
                exn::bail!(CrawlerError {
                    message: format!("size wrong, expect {expected_checksum}, got {checksum}"),
                    status: ErrorStatus::Permanent
                })
            }
            Ok(())
        }
    }
}

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
/// - Repository crawling fails (e.g. invalid URLs or metadata).
/// - A file cannot be downloaded due to network or I/O errors.
/// - The destination directory cannot be created or written to.
/// - File size or checksum validation fails for any file.
/// - Any underlying repository or HTTP client operation fails.
///
///
/// * `P` is A path-like type specifying the destination directory.
pub async fn download_with_validation<P>(
    client: &Client,
    query_repo: QueryRepository,
    dst_dir: P,
) -> Result<(), Exn<CrawlerError>>
// TODO: use DownloadError
where
    P: AsRef<Path>,
{
    // TODO: deal with zip differently according to input instruction

    let repo = query_repo.repo;
    let record_id = query_repo.record_id;
    let root_dir = DirMeta::new_root(repo.as_ref().root_url(&record_id));
    let path = dst_dir.as_ref().join(root_dir.relative());
    fs::create_dir_all(path.as_path()).or_raise(|| CrawlerError {
        message: format!("cannot create dir at '{}'", path.display()),
        status: ErrorStatus::Permanent,
    })?;
    // XXX: ? download_crawled_file_with_validation return its own error type?? can I?
    crawl(client.clone(), Arc::clone(&repo), root_dir)
        .try_for_each_concurrent(10, |entry| {
            let dst_dir = dst_dir.as_ref().to_path_buf();
            async move { download_crawled_file_with_validation(client, entry, &dst_dir).await }
        })
        .await
        .or_raise(|| CrawlerError {
            message: "".to_string(),
            status: ErrorStatus::Permanent,
        })?;
    Ok(())
}
