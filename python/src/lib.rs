#![allow(clippy::needless_pass_by_value)]

use datahugger::{
    crawler::ProgressManager, resolve as inner_resolve, DownloadExt,
    RepositoryRecord as InnerRepositoryRecord,
};
use indicatif::ProgressBar;
use pyo3::{exceptions::PyRuntimeError, prelude::*};
use reqwest::ClientBuilder;
use std::path::PathBuf;

#[pyclass]
struct RepositoryRecord(InnerRepositoryRecord);

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
impl RepositoryRecord {
    #[pyo3(signature = (dst_dir, limit=0))]
    fn download_with_validation(
        self_: PyRef<'_, Self>,
        dst_dir: PathBuf,
        limit: usize,
    ) -> PyResult<()> {
        let user_agent = format!("datahugger-py/{}", env!("CARGO_PKG_VERSION"));
        let client = ClientBuilder::new().user_agent(user_agent).build().unwrap();
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
        let id = self_.0.record_id.clone();
        let repo = self_.0.repo.clone();
        repo.root_url(&id).as_str().into()
    }

    fn id(self_: PyRef<'_, Self>) -> String {
        self_.0.record_id.clone()
    }
}

#[pyfunction]
#[pyo3(signature = (url, /))]
fn resolve(_py: Python, url: &str) -> PyResult<RepositoryRecord> {
    let record = inner_resolve(url).map_err(|err| PyRuntimeError::new_err(format!("{err}")))?;
    Ok(RepositoryRecord(record))
}

#[pymodule]
#[pyo3(name = "datahugger")]
fn datahuggerpy(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(resolve, m)?)?;
    Ok(())
}
