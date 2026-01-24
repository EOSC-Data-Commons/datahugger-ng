#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::type_complexity)] // TODO: type alias
                                   //
                                   // TODO: dedicate exception type for PyRuntimeError.

use datahugger::{
    crawler::{CrawlerError, ProgressManager},
    resolve as inner_resolve, CrawlExt, Dataset, DownloadExt, Entry,
};
use exn::Exn;
use futures_core::stream::BoxStream;
use futures_util::StreamExt;
use indicatif::ProgressBar;
use pyo3::{
    exceptions::{PyRuntimeError, PyStopAsyncIteration, PyStopIteration},
    prelude::*,
    types::PyDict,
};
use pyo3_async_runtimes::tokio::future_into_py;
use reqwest::ClientBuilder;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

#[pyclass]
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

// impl RepositoryRecord {
//     async fn inner_download<P>(
//         self,
//         client: &Client,
//         dst_dir: P,
//         mp: impl ProgressManager,
//         limit: usize,
//     ) -> Result<(), Exn<CrawlerError>>
//     where
//         P: AsRef<Path> + Sync + Send,
//     {
//         todo!()
//     }
// }

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

    // #[pyo3(signature = (dst_dir, limit=0))]
    // fn download(self_: PyRef<'_, Self>, dst_dir: PathBuf, limit: usize) -> PyResult<()> {
    //     let user_agent = format!("datahugger-py/{}", env!("CARGO_PKG_VERSION"));
    //     let client = ClientBuilder::new().user_agent(user_agent).build().unwrap();
    //     let mp = NoProgress;
    //
    //     // blocking call to download, not ideal, but just to sync with original API.
    //     let rt = tokio::runtime::Runtime::new().expect("unable to create tokio runtime");
    //     rt.block_on(async move {
    //         self_
    //             .clone()
    //             .inner_download(&client, dst_dir, mp, limit)
    //             .await
    //     })
    //     .map_err(|err| PyRuntimeError::new_err(format!("{err}")))
    // }

    fn root_url(self_: PyRef<'_, Self>) -> String {
        let repo = self_.0.backend.clone();
        repo.root_url().as_str().into()
    }

    fn crawl(self_: PyRef<'_, Self>) -> PyResult<PyCrawlStream> {
        let user_agent = format!("datahugger-py/{}", env!("CARGO_PKG_VERSION"));
        let client = ClientBuilder::new()
            .user_agent(user_agent)
            .build()
            .map_err(|err| PyRuntimeError::new_err(format!("http client fail: {err}")))?;
        let mp = NoProgress;

        let stream = self_.0.clone().crawl(&client, mp);
        let stream = PyCrawlStream::new(stream);
        Ok(stream)
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

#[pymodule]
#[pyo3(name = "datahugger")]
fn datahuggerpy(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(resolve, m)?)?;
    Ok(())
}

#[pyclass]
struct PyCrawlStream {
    stream: Arc<Mutex<BoxStream<'static, Result<Entry, Exn<CrawlerError>>>>>,
}

impl PyCrawlStream {
    fn new(stream: BoxStream<'static, Result<Entry, Exn<CrawlerError>>>) -> Self {
        PyCrawlStream {
            stream: Arc::new(Mutex::new(stream)),
        }
    }
}

#[derive(Debug)]
struct PyEntry(Entry);

impl<'py> IntoPyObject<'py> for PyEntry {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = std::convert::Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let dict = PyDict::new(py);

        match self.0 {
            Entry::Dir(meta) => {
                dict.set_item("type", "dir").unwrap();
                dict.set_item("path", meta.path.as_str()).unwrap();
                dict.set_item("root_url", meta.root_url.as_str()).unwrap();
                dict.set_item("api_url", meta.api_url.as_str()).unwrap();
            }
            Entry::File(meta) => {
                dict.set_item("type", "file").unwrap();
                dict.set_item("path", meta.path.as_str()).unwrap();
                // dict.set_item("endpoint", meta.endpoint).unwrap();
                dict.set_item("download_url", meta.download_url.as_str())
                    .unwrap();
                dict.set_item("size", meta.size).unwrap();
                // dict.set_item("checksum", meta.checksum).unwrap();
            }
        }

        Ok(dict.into_any())
    }
}

// learn from: https://github.com/developmentseed/obstore/blob/5e4c8341241c3e1491601ea61dd0029f269f4d7e/obstore/src/get.rs#L226
#[pymethods]
impl PyCrawlStream {
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
