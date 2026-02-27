# datahugger

[![crates.io version](https://img.shields.io/crates/v/datahugger.svg)](https://crates.io/crates/datahugger)
[![Rust Docs](https://img.shields.io/docsrs/datahugger?label=docs%20Rust)](https://docs.rs/datahugger)

[![PyPI - Version](https://img.shields.io/pypi/v/datahugger-ng)](https://pypi.org/project/datahugger-ng/)
[![Python Docs](https://img.shields.io/badge/docs-Python%20API-blue)](https://github.com/EOSC-Data-Commons/datahugger-ng/blob/master/python/README.md)



Tool for fetching data from DOI or URL.

Support data repositories:

| Source             | Website                         | Notes | Examples |
|--------------------|---------------------------------|-------| ---------|
| Dataverse          | [dataverse.org](https://dataverse.org/) | [Supported Dataverse repositories](https://github.com/EOSC-Data-Commons/datahugger-ng/blob/master/dataverse-repo-list.md) | [example](#repository-without-limitations) |
| OSF                | [osf.io](https://osf.io/)       | — | [example](#repository-without-limitations) |
| GitHub ✨(new)      | [github.com](https://github.com/) | Use a GitHub API token to get a higher rate limit | [example](#github---avoid-hitting-api-rate-limits-using-a-personal-access-token-pat) |
| Hugging Face ✨(new)| [huggingface.co](https://huggingface.co/) | — | [example](#repository-without-limitations) |
| arXiv              | [arxiv.org](https://arxiv.org/) | — | [example](#repository-without-limitations) |
| Hal                | [hal.science](https://hal.science/) | — | [example](#repository-without-limitations) |
| Zenodo             | [zenodo.org](https://zenodo.org/) | — | [example](#repository-without-limitations) |
| Dryad              | [datadryad.org](https://datadryad.org/) | Bearer token required to download data (see [API instructions](https://datadryad.org/api) for obtaining your API key) | [example](#datadryad-api-key-config-and-download) |
| DataONE            | [dataone.org](https://www.dataone.org/) | [Supported DataONE repositories](https://github.com/EOSC-Data-Commons/datahugger-ng/blob/master/dataone-repo-list.md); requests to its umbrella repositories may be slow | [example](#repository-without-limitations) |


### Install 

prebuilt binaries via shell

```console
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/EOSC-Data-Commons/datahugger-ng/releases/download/v0.4.0/datahugger-installer.sh | sh
```

```console
powershell -ExecutionPolicy Bypass -c "irm https://github.com/EOSC-Data-Commons/datahugger-ng/releases/download/v0.4.0/datahugger-installer.ps1 | iex"
```

```console
brew install unkcpz/tap/datahugger
```

```console
cargo install datahugger
```

For downloading and use python library via,

```console
pip install datahugger-ng
```

## Usage

### CLI

To download all data from a database, run:

```console
datahugger download https://osf.io/3ua2c/ --to /tmp/a-blackhole
```

```console
⠉ Crawling osfstorage/final_model_results_combined/single_species_models_final/niche_additive/Procyon lotor_2025-05-09.rdata...
⠲ Crawling osfstorage/final_model_results_combined/single_species_models_final/niche_additive...
⠈ Crawling osfstorage/final_model_results_combined/single_species_models_final...
⠒ Crawling osfstorage/final_model_results_combined...
⠐ Crawling osfstorage...
o/f/c/event-cbg-intersection.csv   [==>---------------------] 47.20 MB/688.21 MB (   4.92 MB/s,  2m)
o/f/m/a/Corvus corax.pdf           [=======>----------------] 80.47 kB/329.85 kB ( 438.28 kB/s,  1s)
o/f/m/a/Lynx rufus.pdf             [------------------------]      0 B/326.02 kB (       0 B/s,  0s)
o/f/m/a/Ursus arctos.pdf           [------------------------]      0 B/319.05 kB (       0 B/s,  0s)
```

See more examples at [CLI usage examples](#CLI-Examples).

### Python

You can use it as a python library.

```console
pip install datahugger-ng
```

Check [python API docs](https://github.com/EOSC-Data-Commons/datahugger-ng/blob/master/python/README.md) for more examples.

```python
from datahugger import resolve

ds = resolve(
    "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD"
)
ds.download_with_validation(dst_dir=tmp_path, limit=20)

assert sorted([i.name for i in tmp_path.iterdir()]) == [
    "ECM_matrix.py",
    "Markov_comp.py",
    "Markov_learning.py",
    "tutorial1.py",
    "tutorial2.py",
    "tutorial3.py",
    "tutorial4.py",
]
```

The download is very efficient because the underlying Rust implementation leverages all available CPU cores and maximizes the usage your bandwidth.
Use the `limit` parameter to control concurrency; by default, it is set to `0`, which means no limit.

Besides the API for download files in a dataset, we also provide a low-level Python API for implementing custom operations after files are crawled. 
Crawl datasets efficiently and asynchronously with our Rust-powered crawler -- fully utilizing all CPU cores and your network bandwidth.    
Simply resolve a dataset and stream its entries with `async for` and deal with entries concurrently as they arrive:

```python
import asyncio
from datahugger import resolve

async def main():
    ds = resolve(
        "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD"
    )
    async for entry in ds.crawl_file():
        # print or any async operation on the returned entry
        print("crawl:", entry)

asyncio.run(main())
```

## Rust SDK

- `trait DatasetBackend` for adding support for new data repository in your own rust crate.
- `impl Dataset` interface for adding new operations in your own crate. 

## Python SDK

Python SDK mainly for downstream python libraries to implement extra operations on files (e.g. store metadata into DB).

See [python api doc](https://github.com/EOSC-Data-Commons/datahugger-ng/blob/master/python/README.md) for more details.

#### caveats:

Following architecture not yet able to install from pypi.

- target: s390x

## CLI Examples

### GitHub - avoid hitting API rate limits using a Personal Access Token (PAT)

To get higher rate limits, export your [GitHub PAT](https://docs.github.com/en/rest/authentication/authenticating-to-the-rest-api) before downloading:
If you use `gh auth token` to get token if you use `gh` to login in CLI.

https://github.com/EOSC-Data-Commons/datahugger-ng

```bash
export GITHUB_TOKEN="your_personal_access_token" 
datahugger download https://github.com/EOSC-Data-Commons/datahugger-ng --to /tmp/github_download/
```
### Datadryad API key config and download

Datadryad requires a bearer token to access data. First, follow [API instructions](https://datadryad.org/api) to get your key.
You need to have a dryad account and in your profile you can find your API secret, it by default expire in 10 hours.

https://datadryad.org/dataset/doi:10.5061/dryad.mj8m0

```bash
export DRYAD_API_KEY="your_api_key"
datahugger download https://datadryad.org/dataset/doi:10.5061/dryad.mj8m0 --to /tmp/dryad_download/
```

### Datasets without limitations

- Huggingface datasets - simple download

https://huggingface.co/datasets/HuggingFaceFW/finepdfs

```bash
datahugger download https://huggingface.co/datasets/HuggingFaceFW/finepdfs --to /tmp/hf_download/
```

- Dataverse - simple download

https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD

```bash
datahugger download https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD --to /tmp/dataverse_download/
```

- OSF - simple download

```bash
datahugger download https://osf.io/3ua2c --to /tmp/osf_download/ --limit 10
```

- arXiv - simple download

https://arxiv.org/abs/2101.00001v1

```bash
datahugger download https://arxiv.org/abs/2101.00001v1 --to /tmp/arxiv_download/
```

- Zenodo - simple download

https://zenodo.org/records/17867222

```bash
datahugger download https://zenodo.org/record/17867222 --to /tmp/zenodo_download/
```

- Hal.science

```bash
datahugger download https://hal.science/cel-01830944 --to /tmp/hal_download/
```

- DataONE - may be slow for umbrella repositories

https://arcticdata.io/catalog/view/doi%3A10.18739%2FA2542JB2X

```bash
datahugger download https://arcticdata.io/catalog/view/doi%3A10.18739%2FA2542JB2X --to /tmp/dataone_download/
```
- Notes:

- `--to /tmp/...` shows the **download target directory**.  
- `--limit 10` limit the concurrency channel to do polite crawling and downloading.
- Datasets from data repositories which have **rate limits or auth** are highlighted with PAT / API key instructions.  
- Others can be downloaded directly without credentials.  

## Roadmap 

- [x] asynchronously stream file crawling results into the pipeline with exceptional performance
- [x] resolver to resolve url to repository record handlers.
- [x] expressive progress bar when binary running in CLI.
- [x] clear interface to add support for data repositories that lack machine-readable API specifications (e.g., no FAIRiCAT or OAI-PMH support).
- [x] devops, ci with both rust and python tests.
- [x] devops, `json_extract` helper function for serde json value easy value resolve from path.
- [x] clear interface for adding crawling results dealing operations beyond download.
- [x] strong error handling mechanism and logging to avoid interruptions (using `exn` crate).
- [x] Sharable client connection to reduce the cost of repeated reconnections.
- [ ] automatically resolve doi into dateset source url.
- [ ] do detail benchs to show its power (might not need, the cli download already *~1000* times faster for example for dataset https://osf.io/3ua2c/).
- [x] single-pass streaming with computing checksum by plug a hasher in the pipeline.
- [ ] all repos that already supported by py-datahugger
    - [x] Dataone (the repos itself are verry slow in responding http request).
    - [x] Github repo download (support folders collapse and download).
    - [x] zenodo 
    - [x] datadryad
    - [x] arxiv
    - [ ] MendelyDataset
    - [x] HuggingFaceDataset
    - [x] HAL
    - [ ] CERNBox
    - [x] OSFDataset
    - [x] Many Dataverse dataset  
    - [ ] Bgee Database
- [ ] compact but extremly expressive readme
    - [ ] crate.io + python docs.
    - [ ] a bit detail of data repo, shows if fairicat is support etc.
    - [ ] at crate.io, show how to use generics to add new repos or new ops.
- [ ] test python bindings in filemetrix/filefetcher.
- [ ] rust api doc on docs.rs
- [ ] doc on gh-pages?
- [x] python binding (crawl function) that spit out a stream for async use in python side.
- [ ] python binding allow to set HTTP client from a config, or set a token etc.
- [ ] zip extract support.
- [ ] onedata support through signposting, fairicat?
- [ ] not only download, but a versatile metadata fetcher
- [ ] not only download, but scanning to get compute the file type using libmagic.
- [x] one eosc target data repo support that not include in original py-datahugger (HAL?)
- [ ] use this to build a fairicat converter service to dogfooding.
- [x] python bindings
- [x] cli that can do all py-datahugger do.
- [ ] not only local FS, but s3 (using openDAL?)
- [ ] seamephor, config that can intuitively estimate maximum resources been used (already partially taken care by for_each_concurrent limit).
- [ ] supports for less popular data repositories, implement when use cases coming (need your help!)
    - [ ] FigShareDataset (https://api.figshare.com/v2)
    - [ ] DSpaceDataset
    - [ ] SeaNoeDataset
    - [ ] PangaeaDataset
    - [ ] B2ShareDataset
    - [ ] DjehutyDataset

## Development

The development environment can be managed with [devenv](https://devenv.sh/) using nix. 
Enter a full environment with:

```console 
devenv shell -v
```

You can also use your own Rust setup, we don't enforce or test a specific Rust MSRV yet.

### Make new Release

For pypi release:
- update version number at `python/Cargo.toml`. The version don't need to sync with rust crate version.
- trigger manually at CI workflow [`pypi-publish`](https://github.com/EOSC-Data-Commons/datahugger-ng/actions/workflows/pypi-publish.yaml)

For binary release and for crates.io release, they share same version number.

```console
# commit and push to main (can be done with a PR)
git commit -am "release: version 0.1.0"
git push

# actually push the tag up (this triggers dist's CI)
git tag v0.1.0
git push --tags
```

CI workflow of crates.io build can be trigger manually at CI workflow [`crate-publish`](https://github.com/EOSC-Data-Commons/datahugger-ng/actions/workflows/crate-publish.yaml).
But it will not run the final crates.io upload.

## License

All contributions must retain this attribution.

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

