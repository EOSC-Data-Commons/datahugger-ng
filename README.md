# datahugger

Tool for fetching data from DOI or URL.

Support data repositories:

| Source        | Website                         | Notes |
|---------------|----------------------------------|-------|
| Dataverse     | https://dataverse.org/           | [Supported Dataverse repositories](https://github.com/EOSC-Data-Commons/datahugger-rs/blob/master/dataverse-repo-list.md) |
| OSF           | https://osf.io/                  | — |
| GitHub        | https://github.com/              | Use a GitHub API token to get a higher rate limit |
| arXiv         | https://arxiv.org/               | — |
| Dryad         | https://datadryad.org            | Requires a bearer token to download data ([API instructions](https://datadryad.org/api)) |
| DataONE       | https://www.dataone.org/         | [Supported DataONE repositories](https://github.com/EOSC-Data-Commons/datahugger-rs/blob/master/dataone-repo-list.md); requests to umbrella repositories may be slow |


## Usage

### CLI

download the binary or brew, apt, curl..

To download all data from a database, run:

```console
datahugger download https://osf.io/3ua2c/
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

### Python

You can use it as a python library.

```python
from datahugger_ng import resolve

record = resolve(
    "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD"
)
record.download_with_validation(tmp_path)

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

## License

All contributions must retain this attribution.

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

