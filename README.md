# datahugger-rs

- keep folder hierachy.
- async
- zero-copy
- direct streaming to dst.
- one go checksum validation

## roadmap

before public and 1st release

- [ ] all repos that already supported by py-datahugger
- [ ] one eosc target data repo support that not include in original py-datahugger
- [ ] python bindings
- [ ] cli that can do all py-datahugger do.
- [ ] not only download, but a versatile metadata fetcher
- [ ] not only local FS, but s3
- [ ] seamephor, config that can intuitively estimate maximum resources been used.
- [ ] do benchs to show its power.

## bench

- [ ] with/without after download checksum validation
- [ ] dataset with large files.
- [ ] dataset with ~100 files.
- [ ] full download on a real data repo.
- [ ] every bench test run for two types: pure cli call and wrapped python api 
