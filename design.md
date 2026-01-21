# datahugger-rs

- keep folder hierachy.
- async
- zero-copy
- direct streaming to dst.
- one go checksum validation

## roadmap

before public and 1st release

- [ ] all repos that already supported by py-datahugger
- [ ] compact but extremly expressive readme
    - [ ] crate.io + python docs.
    - [ ] a bit detail of data repo, shows if fairicat is support etc.
    - [ ] at crate.io, show how to use generics to add new repos or new ops.
- [ ] python binding (crawl function) that spit out a stream for async use in python side.
- [ ] onedata support.
- [ ] not only download, but a versatile metadata fetcher
- [ ] one eosc target data repo support that not include in original py-datahugger
- [ ] use this to build a fairicat converter service to dogfooding.
- [x] python bindings
- [x] cli that can do all py-datahugger do.
- [ ] not only local FS, but s3 (using openDAL?)
- [ ] seamephor, config that can intuitively estimate maximum resources been used (already partially taken care by for_each_concurrent limit).
- [ ] do benchs to show its power.

## bench

- [ ] with/without after download checksum validation
- [ ] dataset with large files.
- [ ] dataset with ~100 files.
- [ ] full download on a real data repo.
- [ ] every bench test run for two types: pure cli call and wrapped python api 

## notes

- [x] maybe add support to windows, it is not now because CrawlPath is using '/'. (boundary is take care by Path)
- [ ] minimize the maintenance effort by having auto remove data repo validation and fire issues.
- [ ] happing above auto validation and publish the result in the gh-page.
- [ ] have clear data repo onboarding instruction (one trait to impl).
- [ ] have clear new data repo request issue template.
