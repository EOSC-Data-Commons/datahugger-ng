mod arxiv;
mod dataone;
mod dataverse;
mod dryad;
mod github;
mod hal;
mod huggingface;
mod osf;
mod zenodo;

pub use arxiv::Arxiv;
pub use dataone::Dataone;
pub use dataverse::{DataverseDataset, DataverseFile};
pub use dryad::DataDryad;
pub use github::GitHub;
pub use hal::HalScience;
pub use huggingface::HuggingFace;
pub use osf::OSF;
pub use zenodo::Zenodo;
