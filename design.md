# datahugger-ng

- keep folder hierachy.
- async
- zero-copy
- direct streaming to dst.
- one go checksum validation

## roadmap

before public and 1st release

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
