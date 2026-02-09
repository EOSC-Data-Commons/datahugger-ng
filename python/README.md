# Datahugger API doc

![Python version](https://img.shields.io/badge/python-%3E%3D%203.10-blue)

This module provides a unified interface to **resolve**, **crawl**, and **download** datasets exposed over HTTP-like endpoints.
A key design goal is that dataset crawling can be consumed **both synchronously and asynchronously** using the same API.

## Overview

* Resolve a dataset from a URL
* Crawl its contents as a stream of entries (files or directories)
* Download and validate dataset contents using a blocking API backed by an async runtime

## Core Concepts

### `DirEntry`

Represents a directory in the dataset.

```python
@dataclass
class DirEntry(Entry):
    path_crawl_rel: pathlib.Path
    root_url: str
    api_url: str
```

#### Fields

- `path_crawl_rel`
  Path of the directory relative to the dataset root.

- `root_url`
  Root URL of the dataset this directory belongs to.

- `api_url`
  API endpoint used to query the directory contents.

### `FileEntry`

Represents a file in the dataset.

```python
@dataclass
class FileEntry(Entry):
    path_crawl_rel: pathlib.Path
    download_url: str
    size: int | None
    checksum: list[tuple[str, str]]
```

#### Fields

- `path_crawl_rel`
  Path of the file relative to the dataset root.

- `download_url`
  URL from which the file can be downloaded.

- `size`
  File size in bytes, if known.

- `checksum`
  List of checksum pairs `(algorithm, value)`
  (e.g. `("sha256", "...")`).

## Iteration Model

### `SyncAsyncIterator[T]`

A protocol that allows a single object to be used as **both a synchronous and an asynchronous iterator**.

```python
class SyncAsyncIterator(Protocol[T]):
    def __aiter__(self) -> AsyncIterator[T]: ...
    async def __anext__(self) -> T: ...
    def __iter__(self) -> Iterator[T]: ...
    def __next__(self) -> T: ...
```

This enables APIs that can be consumed in either context without duplication.

## Dataset

The central abstraction representing a remote dataset.

```python
class Dataset:
    def crawl(self) -> SyncAsyncIterator[FileEntry | DirEntry]: ...
    def crawl_file(self) -> SyncAsyncIterator[FileEntry]: ...
    def download_with_validation(
        self, dst_dir: pathlib.Path, limit: int = 0
    ) -> None: ...
    def id(self) -> str: ...
    def root_url(self) -> str: ...
```

### `Dataset.crawl()`

```python
def crawl(self) -> SyncAsyncIterator[FileEntry | DirEntry]
```

Returns a stream of dataset entries (optional type that can be either `DirEntry` or `FileEntry`).

The returned object supports **both**:

#### Synchronous iteration

```python
for entry in dataset.crawl():
    print(entry)
```

#### Asynchronous iteration

```python
async for entry in dataset.crawl():
    print(entry)
```

Entries are yielded as either `DirEntry` or `FileEntry`.

### `Dataset.download_with_validation()`

```python
def download_with_validation(
    self, dst_dir: pathlib.Path, limit: int = 0
) -> None
```

Downloads files in the dataset into the given directory and validates them using the provided checksums.

* This is a **blocking** call.
* Internally backed by a Rust async runtime.
* Intended for use from synchronous Python code.

#### Parameters

* **`dst_dir`**
  Destination directory for downloaded files.

* **`limit`**
  Maximum number of files to download.
  `0` means no limit.

### `Dataset.root_url()`

```python
def root_url(self) -> str
```

Returns the dataset’s root URL.

## Resolving a Dataset

### `resolve`

```python
def resolve(url: str, /) -> Dataset
```

Resolves a dataset from a given URL.

#### Example

```python
dataset = resolve("https://example.com/dataset")
```

The returned `Dataset` can then be crawled or downloaded.

## Example Usage

### Crawl a dataset synchronously

```python
dataset = resolve("https://example.com/dataset")

for entry in dataset.crawl():
    if isinstance(entry, FileEntry):
        print("File:", entry.path_crawl_rel)
    elif isinstance(entry, DirEntry):
        print("Dir:", entry.path_crawl_rel)
```

### Crawl a dataset asynchronously

```python
dataset = resolve("https://example.com/dataset")

async for entry in dataset.crawl():
    print(entry)
```

### Download a dataset

```python
dataset = resolve("https://example.com/dataset")
dataset.download_with_validation(dst_dir=pathlib.Path("./data"))
```
