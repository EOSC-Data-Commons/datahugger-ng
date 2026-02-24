// TODO: dedicate exception type for PyRuntimeError.
#![allow(clippy::needless_pass_by_value)]
// TODO: type alias
#![allow(clippy::type_complexity)]

// XXX: not safe deprecate init_ssl_cert_env_vars
#[cfg(feature = "openssl-vendored")]
fn probe_ssl_certs() {
    openssl_probe::init_ssl_cert_env_vars();
}

#[cfg(not(feature = "openssl-vendored"))]
fn probe_ssl_certs() {}

#[pyfunction]
pub fn main() {
    probe_ssl_certs();
}

use datahugger::{
    crawl,
    crawler::{CrawlerError, ProgressManager},
    resolve as inner_resolve, resolve_doi_to_url as inner_resolve_doi_to_url, CrawlExt, Dataset,
    DownloadExt, Entry, FileMeta,
};
use exn::Exn;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use indicatif::ProgressBar;
use pyo3::{
    exceptions::{PyRuntimeError, PyStopAsyncIteration, PyStopIteration},
    prelude::*,
};
use pyo3::{ffi::c_str, types::PyDict};
use pyo3_async_runtimes::tokio::future_into_py;
use reqwest::{Client, ClientBuilder};
use std::{path::PathBuf, sync::Arc};
use std::time::Duration;
use reqwest::redirect::Policy;
use tokio::sync::Mutex;

pub trait CrawlFileExt {
    fn crawl_file(
        self,
        client: &Client,
        mp: impl ProgressManager,
    ) -> BoxStream<'static, Result<FileMeta, Exn<CrawlerError>>>;
}

impl CrawlFileExt for Dataset {
    fn crawl_file(
        self,
        client: &Client,
        mp: impl ProgressManager,
    ) -> BoxStream<'static, Result<FileMeta, Exn<CrawlerError>>> {
        let root_dir = self.root_dir();
        crawl(
            client.clone(),
            Arc::clone(&self.backend),
            root_dir,
            mp.clone(),
        )
        .filter_map(|res| async move {
            match res {
                Ok(Entry::Dir(_)) => None,
                Ok(Entry::File(f)) => Some(Ok(f)),
                Err(e) => Some(Err(e)),
            }
        })
        .boxed()
    }
}

#[pyclass]
#[pyo3(name = "Dataset")]
#[derive(Clone)]
struct PyDataset(Dataset);

#[derive(Clone)]
struct NoProgress;

impl ProgressManager for NoProgress {
    fn insert(&self, _index: usize, _pb: ProgressBar) -> ProgressBar {
        ProgressBar::hidden()
    }

    fn insert_from_back(&self, _index: usize, _pb: ProgressBar) -> ProgressBar {
        ProgressBar::hidden()
    }
}

#[pymethods]
impl PyDataset {
    #[pyo3(signature = (dst_dir, limit=0))]
    fn download_with_validation(
        self_: PyRef<'_, Self>,
        dst_dir: PathBuf,
        limit: usize,
    ) -> PyResult<()> {
        let user_agent = format!("datahugger-py/{}", env!("CARGO_PKG_VERSION"));
        let client = ClientBuilder::new()
            .user_agent(user_agent)
            .build()
            .map_err(|err| PyRuntimeError::new_err(format!("http client fail: {err}")))?;
        let mp = NoProgress;

        // blocking call to download, not ideal, but just to sync with original API.
        let rt = tokio::runtime::Runtime::new().expect("unable to create tokio runtime");
        rt.block_on(async move {
            self_
                .0
                .clone()
                .download_with_validation(&client, dst_dir, mp, limit)
                .await
        })
        .map_err(|err| PyRuntimeError::new_err(format!("{err}")))
    }

    fn root_url(self_: PyRef<'_, Self>) -> String {
        let repo = self_.0.backend.clone();
        repo.root_url().as_str().into()
    }

    fn crawl(self_: PyRef<'_, Self>) -> PyResult<PyEntryStream> {
        let user_agent = format!("datahugger-py/{}", env!("CARGO_PKG_VERSION"));
        let client = ClientBuilder::new()
            .user_agent(user_agent)
            .build()
            .map_err(|err| PyRuntimeError::new_err(format!("http client fail: {err}")))?;
        let mp = NoProgress;

        let stream = self_.0.clone().crawl(&client, mp);
        let stream = PyEntryStream::new(stream);
        Ok(stream)
    }

    fn crawl_file(self_: PyRef<'_, Self>) -> PyResult<PyFileMetaStream> {
        let user_agent = format!("datahugger-py/{}", env!("CARGO_PKG_VERSION"));
        let client = ClientBuilder::new()
            .user_agent(user_agent)
            .build()
            .map_err(|err| PyRuntimeError::new_err(format!("http client fail: {err}")))?;
        let mp = NoProgress;

        let stream = self_.0.clone().crawl_file(&client, mp);
        let stream = PyFileMetaStream::new(stream);
        Ok(stream)
    }
}

#[pyclass]
struct DOIResolver {
    runtime: tokio::runtime::Runtime,
    client: Client,
}

#[pymethods]
impl DOIResolver {
    #[new]
    #[pyo3(signature = (timeout=5))]
    fn new(timeout: u64) -> PyResult<Self> {
        Ok(Self {
            runtime: tokio::runtime::Runtime::new()
                .map_err(|err| PyRuntimeError::new_err(format!("failed to create runtime: {err}")))?,
            client: Client::builder()
                .use_native_tls()
                .timeout(Duration::from_secs(timeout))
                .redirect(Policy::limited(5)) // limit number of redirects (relevant if follow_redirects is set to true)
                .build()
                .map_err(|err| PyRuntimeError::new_err(format!("failed to create client: {err}")))?,
        })
    }

    #[pyo3(signature = (doi, follow_redirects=true))]
    fn resolve(&self, doi: String, follow_redirects: bool) -> PyResult<String> {
        self.runtime
            .block_on(inner_resolve_doi_to_url(&self.client, &doi, follow_redirects))
            .map_err(|err| PyRuntimeError::new_err(format!("{err}")))
    }

    #[pyo3(signature = (dois, follow_redirects=true))]
    fn resolve_many(&self, dois: Vec<String>, follow_redirects: bool) -> PyResult<Vec<String>> {
        let futures = dois.iter().map(|doi| inner_resolve_doi_to_url(&self.client, doi, follow_redirects));
        self.runtime
            .block_on(futures::future::join_all(futures))
            .into_iter()
            .collect::<Result<Vec<String>, _>>()
            .map_err(|err| PyRuntimeError::new_err(format!("{err}")))
    }
}

#[pyfunction]
#[pyo3(signature = (url, /))]
fn resolve(_py: Python, url: &str) -> PyResult<PyDataset> {
    let rt = tokio::runtime::Runtime::new().unwrap(); // create a runtime
    let ds = rt
        .block_on(inner_resolve(url))
        .map_err(|err| PyRuntimeError::new_err(format!("{err}")))?;
    Ok(PyDataset(ds))
}

#[pyclass]
struct PyEntryStream {
    stream: Arc<Mutex<BoxStream<'static, Result<Entry, Exn<CrawlerError>>>>>,
}

impl PyEntryStream {
    fn new(stream: BoxStream<'static, Result<Entry, Exn<CrawlerError>>>) -> Self {
        PyEntryStream {
            stream: Arc::new(Mutex::new(stream)),
        }
    }
}

#[pyclass]
struct PyFileMetaStream {
    stream: Arc<Mutex<BoxStream<'static, Result<FileMeta, Exn<CrawlerError>>>>>,
}

impl PyFileMetaStream {
    fn new(stream: BoxStream<'static, Result<FileMeta, Exn<CrawlerError>>>) -> Self {
        PyFileMetaStream {
            stream: Arc::new(Mutex::new(stream)),
        }
    }
}

#[pyclass]
#[pyo3(name = "Entry", subclass)]
struct PyEntryBase;

#[pymethods]
impl PyEntryBase {
    #[new]
    fn new() -> Self {
        PyEntryBase
    }
}

#[pyclass]
#[pyo3(name = "DirEntry", extends=PyEntryBase)]
struct PyDirEntry {
    #[pyo3(get)]
    path_crawl_rel: PathBuf,
    #[pyo3(get)]
    root_url: String,
    #[pyo3(get)]
    api_url: String,
}

#[pyclass]
#[pyo3(name = "FileEntry", extends=PyEntryBase)]
struct PyFileEntry {
    #[pyo3(get, set)]
    path_crawl_rel: PathBuf,
    #[pyo3(get, set)]
    download_url: String,
    #[pyo3(get, set)]
    size: Option<u64>,
    #[pyo3(get, set)]
    checksum: Vec<(String, String)>,
}

#[pymethods]
impl PyFileEntry {
    #[new]
    fn new(
        path_crawl_rel: PathBuf,
        download_url: String,
        size: Option<u64>,
        checksum: Vec<(String, String)>,
    ) -> (Self, PyEntryBase) {
        (
            PyFileEntry {
                path_crawl_rel,
                download_url,
                size,
                checksum,
            },
            PyEntryBase::new(),
        )
    }
}

#[derive(Debug)]
struct PyEntry(Entry);

impl<'py> IntoPyObject<'py> for PyEntry {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let obj = match self.0 {
            Entry::Dir(meta) => Py::new(
                py,
                (
                    PyDirEntry {
                        path_crawl_rel: PathBuf::from(meta.path.as_str()),
                        root_url: meta.root_url.as_str().to_string(),
                        api_url: meta.api_url.as_str().to_string(),
                    },
                    PyEntryBase,
                ),
            )
            .map(pyo3::Py::into_any)
            .expect("cannot construct the PyDirEntry"),
            Entry::File(meta) => Py::new(
                py,
                (
                    PyFileEntry {
                        path_crawl_rel: PathBuf::from(meta.path.as_str()),
                        download_url: meta.download_url.as_str().to_string(),
                        size: meta.size,
                        checksum: meta
                            .checksum
                            .iter()
                            .map(|cs| match cs {
                                datahugger::Checksum::Md5(v) => ("md5".to_string(), v.clone()),
                                datahugger::Checksum::Sha256(v) => {
                                    ("sha256".to_string(), v.clone())
                                }
                            })
                            .collect::<Vec<_>>(),
                    },
                    PyEntryBase,
                ),
            )
            .map(pyo3::Py::into_any)
            .expect("cannot construct the PyDirEntry"),
        };

        Ok(obj.into_bound(py))
    }
}

#[derive(Debug)]
struct PyFileMeta(FileMeta);

impl<'py> IntoPyObject<'py> for PyFileMeta {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let meta = self.0;
        let obj = Py::new(
            py,
            (
                PyFileEntry {
                    path_crawl_rel: PathBuf::from(meta.path.as_str()),
                    download_url: meta.download_url.as_str().to_string(),
                    size: meta.size,
                    checksum: meta
                        .checksum
                        .iter()
                        .map(|cs| match cs {
                            datahugger::Checksum::Md5(v) => ("md5".to_string(), v.clone()),
                            datahugger::Checksum::Sha256(v) => ("sha256".to_string(), v.clone()),
                        })
                        .collect::<Vec<_>>(),
                },
                PyEntryBase,
            ),
        )
        .map(pyo3::Py::into_any)
        .expect("cannot construct the PyDirEntry");

        Ok(obj.into_bound(py))
    }
}

// learn from:
// https://github.com/developmentseed/obstore/blob/5e4c8341241c3e1491601ea61dd0029f269f4d7e/obstore/src/get.rs#L226
#[pymethods]
impl PyEntryStream {
    fn __aiter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();

        future_into_py(py, next_stream(stream, false))
    }

    fn __next__(&self, _py: Python<'_>) -> PyResult<PyEntry> {
        let runtime = pyo3_async_runtimes::tokio::get_runtime();
        let stream = self.stream.clone();
        runtime.block_on(next_stream(stream, true))
    }
}

async fn next_stream(
    stream: Arc<Mutex<BoxStream<'static, Result<Entry, Exn<CrawlerError>>>>>,
    is_sync: bool,
) -> PyResult<PyEntry> {
    let mut stream = stream.lock().await;
    match stream.next().await {
        Some(Ok(entry)) => {
            let py_entry = PyEntry(entry);
            Ok(py_entry)
        }
        // TODO: Errors mapping to py types as well and return the PyCrawrError.
        Some(Err(e)) => Err(PyRuntimeError::new_err(format!("{e:?}"))),
        None => {
            if is_sync {
                Err(PyStopIteration::new_err("stream exhausted"))
            } else {
                Err(PyStopAsyncIteration::new_err("stream exhausted"))
            }
        }
    }
}

#[pymethods]
impl PyFileMetaStream {
    fn __aiter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let stream = self.stream.clone();

        future_into_py(py, next_stream_file(stream, false))
    }

    fn __next__(&self, _py: Python<'_>) -> PyResult<PyFileMeta> {
        let runtime = pyo3_async_runtimes::tokio::get_runtime();
        let stream = self.stream.clone();
        runtime.block_on(next_stream_file(stream, true))
    }
}

async fn next_stream_file(
    stream: Arc<Mutex<BoxStream<'static, Result<FileMeta, Exn<CrawlerError>>>>>,
    is_sync: bool,
) -> PyResult<PyFileMeta> {
    let mut stream = stream.lock().await;
    match stream.next().await {
        Some(Ok(fm)) => {
            let frame = PyFileMeta(fm);
            Ok(frame)
        }
        // TODO: Errors mapping to py types as well and return the PyCrawrError.
        Some(Err(e)) => Err(PyRuntimeError::new_err(format!("{e:?}"))),
        None => {
            if is_sync {
                Err(PyStopIteration::new_err("stream exhausted"))
            } else {
                Err(PyStopAsyncIteration::new_err("stream exhausted"))
            }
        }
    }
}

#[pymodule]
#[pyo3(name = "datahugger")]
fn datahuggerpy(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(resolve, m)?)?;
    m.add_class::<DOIResolver>()?;
    m.add_class::<PyDataset>()?;
    m.add_class::<PyEntryBase>()?;

    // Dir
    let dir = py.get_type::<PyDirEntry>();
    let ann = PyDict::new(py);
    ann.set_item("path_crawl_rel", py.get_type::<pyo3::types::PyString>())?;
    ann.set_item("root_url", py.get_type::<pyo3::types::PyString>())?;
    ann.set_item("api_url", py.get_type::<pyo3::types::PyString>())?;
    dir.setattr("__annotations__", ann)?;
    py.import("dataclasses")?
        .getattr("dataclass")?
        .call1((dir,))?;
    // File
    let f = py.get_type::<PyFileEntry>();
    let ann = PyDict::new(py);
    ann.set_item("path_crawl_rel", py.get_type::<pyo3::types::PyString>())?;
    ann.set_item("download_url", py.get_type::<pyo3::types::PyString>())?;
    let size_type = py.eval(c_str!("int | None"), None, None)?;
    ann.set_item("size", size_type)?;
    let checksum_type = py.eval(c_str!("list[tuple[str, str]]"), None, None)?;
    ann.set_item("checksum", checksum_type)?;
    f.setattr("__annotations__", ann)?;
    py.import("dataclasses")?
        .getattr("dataclass")?
        .call1((f,))?;

    m.add_class::<PyDirEntry>()?;
    m.add_class::<PyFileEntry>()?;
    Ok(())
}
