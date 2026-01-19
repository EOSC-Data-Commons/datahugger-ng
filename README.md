# datahugger-rs

- keep folder hierachy.
- async
- zero-copy
- direct streaming to dst.
- one go checksum validation

## roadmap

before public and 1st release

- [ ] all repos that already supported by py-datahugger
- [ ] onedata support.
- [ ] one eosc target data repo support that not include in original py-datahugger
- [ ] python bindings
- [ ] cli that can do all py-datahugger do.
- [ ] not only download, but a versatile metadata fetcher
- [ ] not only local FS, but s3
- [ ] seamephor, config that can intuitively estimate maximum resources been used.
- [ ] do benchs to show its power.
- [ ] compact but extremly expressive readme
- [ ] use this to build a fairicat converter service to dogfooding.

## bench

- [ ] with/without after download checksum validation
- [ ] dataset with large files.
- [ ] dataset with ~100 files.
- [ ] full download on a real data repo.
- [ ] every bench test run for two types: pure cli call and wrapped python api 

## notes

- [ ] maybe add support to windows, it is not now because CrawlPath is using '/'.
- [ ] minimize the maintenance effort by having auto remove data repo validation and fire issues.
- [ ] happing above auto validation and publish the result in the gh-page.
- [ ] have clear data repo onboarding instruction (one trait to impl).
- [ ] have clear new data repo request issue template.
