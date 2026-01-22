# datahugger

Tool for fetching data from DOI or URL.

Support data repositories:

| Source        | Website                         | Notes |
|---------------|----------------------------------|-------|
| Dataverse     | https://dataverse.org/           | [Supported Dataverse repositories](https://github.com/EOSC-Data-Commons/datahugger-rs/blob/master/dataverse-repo-list.md) |
| OSF           | https://osf.io/                  | — |
| GitHub        | https://github.com/              | Use a GitHub API token to get a higher rate limit |
| arXiv         | https://arxiv.org/               | — |
| Zenodo        | https://zenodo.org/              | — |
| Dryad         | https://datadryad.org            | Bearer token required to download data (see [API instructions](https://datadryad.org/api) for how to your api key) |
| DataONE       | https://www.dataone.org/         | [Supported DataONE repositories](https://github.com/EOSC-Data-Commons/datahugger-rs/blob/master/dataone-repo-list.md); requests to umbrella repositories may be slow |


## Usage

### CLI

download the binary or brew, apt, curl..

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

```python
from datahugger_ng import resolve

record = resolve(
    "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD"
)
record.download_with_validation(dst_dir=tmp_path)

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

## Rust SDK

- `trait Repository` for adding support for new data repository in your own rust crate.
- `impl RepositoryRecord` interface for adding new operations in your own crate. 

## Python SDK

Python SDK mainly for downstream python libraries to implement extra operations on files (e.g. store metadata into DB).

## CLI Examples

### Github repo download

...

### Datadryad API key config and download

...

### Download for requests unlimited data repositories

...

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
- [ ] do detail benchs to show its power (might not need, the cli download already ~1000 times faster for example dataset https://osf.io/3ua2c/).
- [x] single-pass streaming with computing checksum by plug a hasher in the pipeline.
- [ ] all repos that already supported by py-datahugger
    - [x] Dataone (the repos itself are verry slow in responding http request).
    - [x] Github repo download (support folders collapse and download).
    - [x] zenodo 
    - [x] datadryad
    - [x] arxiv
    - [ ] MendelyDataset
    - [ ] HuggingFaceDataset
    - [ ] HAL
    - [ ] CERNBox
    - [x] OSFDataset
    - [x] Many Dataverse dataset  
    - [ ] Bgee Database
- [ ] compact but extremly expressive readme
    - [ ] crate.io + python docs.
    - [ ] a bit detail of data repo, shows if fairicat is support etc.
    - [ ] at crate.io, show how to use generics to add new repos or new ops.
- [ ] doc on gh-pages?
- [ ] python binding (crawl function) that spit out a stream for async use in python side.
- [ ] python binding allow to set HTTP client from a config, or set a token etc.
- [ ] zip extract support.
- [ ] onedata support through signposting, fairicat?
- [ ] not only download, but a versatile metadata fetcher
- [ ] one eosc target data repo support that not include in original py-datahugger (HAL?)
- [ ] use this to build a fairicat converter service to dogfooding.
- [x] python bindings
- [ ] test python bindings in filemetrix/filefetcher.
- [x] cli that can do all py-datahugger do.
- [ ] not only local FS, but s3 (using openDAL?)
- [ ] seamephor, config that can intuitively estimate maximum resources been used (already partially taken care by for_each_concurrent limit).
- [ ] suuports for less popular data repositories, implement when use cases coming (need your help!)
    - [ ] FigShareDataset (https://api.figshare.com/v2)
    - [ ] DSpaceDataset
    - [ ] SeaNoeDataset
    - [ ] PangaeaDataset
    - [ ] B2ShareDataset
    - [ ] DjehutyDataset

## License

All contributions must retain this attribution.

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

