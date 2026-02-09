from dataclasses import dataclass
import pathlib

from typing import Protocol, TypeVar, AsyncIterator, Iterator

T = TypeVar("T", covariant=True)

class SyncAsyncIterator(Protocol[T]):
    def __aiter__(self) -> AsyncIterator[T]: ...
    async def __anext__(self) -> T: ...
    def __iter__(self) -> Iterator[T]: ...
    def __next__(self) -> T: ...

class Entry(object):
    """base entry for file and dir"""

@dataclass
class DirEntry(Entry):
    path_crawl_rel: pathlib.Path
    root_url: str
    api_url: str

@dataclass
class FileEntry(Entry):
    path_crawl_rel: pathlib.Path
    download_url: str
    size: int | None
    checksum: list[tuple[str, str]]

class Dataset(object):
    def download_with_validation(self, dst_dir: pathlib.Path, limit: int = 0) -> None:
        """blocking call, using rust's async runtime"""
    def crawl_file(self) -> SyncAsyncIterator[FileEntry]:
        """return a stream that can be either sync or async iterator over `FileEntry`"""
    def crawl(self) -> SyncAsyncIterator[FileEntry | DirEntry]:
        """return a stream that can be either sync or async iterator over `FileEntry | DirEntry`"""
    def root_url(self) -> str: ...

def resolve(url: str, /) -> Dataset: ...
