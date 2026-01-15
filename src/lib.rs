mod repo;

use anyhow::Context;
use anyhow::anyhow;
use bytes::Buf;
use digest::Digest;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use reqwest::{Client, ClientBuilder};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::sync::Arc;
use std::{fs, path::Path};
use tokio::{fs::OpenOptions, io::AsyncWriteExt};
use tracing::{info, instrument};

use url::Url;

use crate::repo::DirMeta;
use crate::repo::Entry;
use crate::repo::OSF;
use crate::repo::crawl;

enum Hasher {
    Md5(md5::Md5),
    Sha256(sha2::Sha256),
}

impl Hasher {
    fn update(&mut self, data: &[u8]) {
        match self {
            Hasher::Md5(h) => h.update(data),
            Hasher::Sha256(h) => h.update(data),
        }
    }

    fn finalize(self) -> Vec<u8> {
        match self {
            Hasher::Md5(h) => h.finalize().to_vec(),
            Hasher::Sha256(h) => h.finalize().to_vec(),
        }
    }
}

// this function follow the path `xp` which is a `.` split string on the serde_json::Value to get
// the final value and deserialize it to the expected type. The errors can be caused by:
// 1. the path not lead to any value
// 2. unable to deserialize to the expected type
fn json_get<T>(value: &Value, xp: &str) -> anyhow::Result<T>
where
    T: DeserializeOwned,
{
    let mut current = value;

    for key in xp.split('.').filter(|s| !s.is_empty()) {
        current = match current {
            Value::Object(map) => map
                .get(key)
                .with_context(|| format!("path element '{key}' not found"))?,
            Value::Array(arr) => {
                let idx: usize = key
                    .parse()
                    .with_context(|| format!("expected array index, got '{key}'"))?;
                arr.get(idx)
                    .with_context(|| format!("array index {idx} out of bounds"))?
            }
            _ => {
                return Err(anyhow!(
                    "cannot descend into non-container value at '{key}'",
                ));
            }
        };
    }
    serde_json::from_value(current.clone()).context("failed to deserialize value at final path")
}

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

#[instrument(skip(client))]
async fn download_file_with_validation<P>(client: &Client, src: Entry, dst: P) -> anyhow::Result<()>
where
    P: AsRef<Path> + std::fmt::Debug,
{
    info!("downloading with validating");
    match src {
        Entry::Dir(dir_meta) => {
            let path = dst.as_ref().join(dir_meta.path);
            fs::create_dir_all(path)?;
            Ok(())
        }
        Entry::File(file_meta) => {
            // prepare stream src
            let resp = client
                .get(file_meta.download_url)
                .send()
                .await?
                .error_for_status()?;
            let mut stream = resp.bytes_stream();
            // prepare file dst
            let path = dst.as_ref().join(file_meta.path);
            info!("{:?}", path);
            let mut fh = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .await?;

            let checksum = file_meta
                .checksum
                .iter()
                .filter(|c| matches!(c, repo::Checksum::Sha256(_)))
                .collect::<Vec<_>>()[0];
            let (mut hasher, expected_checksum) = match checksum {
                repo::Checksum::Sha256(value) => (Hasher::Sha256(sha2::Sha256::new()), value),
                repo::Checksum::Md5(value) => (Hasher::Md5(md5::Md5::new()), value),
            };
            let expected_size = file_meta.size.context("missing size")?;
            let mut got_size = 0;

            while let Some(item) = stream.next().await {
                let mut bytes = item?;
                let chunk = bytes.chunk();
                hasher.update(chunk);
                got_size += bytes.len() as u64;
                fh.write_all_buf(&mut bytes).await?;
            }

            if got_size != expected_size {
                anyhow::bail!("size wrong")
            }

            let checksum = hasher.finalize();
            if hex::encode(checksum) != *expected_checksum {
                anyhow::bail!("checksum wrong")
            }
            Ok(())
        }
    }
}

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

/// download files resolved from a url into a folder.
/// with validating checksum and the download size for every file .
/// # Errors
/// ???
pub async fn download_with_validation<P>(root_url: Url, dst_dir: P) -> anyhow::Result<()>
where
    P: AsRef<Path>,
{
    // TODO: deal with zip differently according to input instruction

    let client = ClientBuilder::new().build()?;
    let repo = Arc::new(OSF::new(client.clone()));
    // FIXME: can I start with "/" or "ROOT"?
    let root_dir = DirMeta::new(root_url, ".");
    crawl(repo, root_dir)
        .try_for_each_concurrent(20, |f| {
            let dst_dir = dst_dir.as_ref().to_path_buf();
            let client = client.clone();
            async move { download_file_with_validation(&client, f, &dst_dir).await }
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_json_get_default() {
        let value = json!({
            "data": [
                { "name": "bob", "num": 5 }
            ]
        });
        let xp = "data.0.name";
        let v: String = json_get(&value, xp).unwrap();
        assert_eq!(v, "bob");

        let xp = "data.0.num";
        let v: u64 = json_get(&value, xp).unwrap();
        assert_eq!(v, 5);
    }

    #[test]
    fn test_json_get_missing_path() {
        let value = serde_json::json!({
            "data": []
        });

        let xp = "data.0.name";
        let err = json_get::<String>(&value, xp).unwrap_err();
        assert!(err.to_string().contains("out of bounds"));
    }

    #[test]
    fn test_json_get_wrong_container() {
        let value = serde_json::json!({
            "data": "not an array"
        });

        let xp = "data.0";
        let err = json_get::<String>(&value, xp).unwrap_err();
        assert!(err.to_string().contains("cannot descend"));
    }

    #[test]
    fn test_json_get_deserialize_error() {
        let value = serde_json::json!({
            "data": { "id": "not a number" }
        });

        let xp = "data.id";
        let err = json_get::<i64>(&value, xp).unwrap_err();
        assert!(err.to_string().contains("deserialize"));
    }
}
