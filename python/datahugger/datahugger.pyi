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
    filename: str | None
    file_identifier: str | None
    path_crawl_rel: pathlib.Path
    download_url: str
    size: int | None
    checksum: list[tuple[str, str]]
    mimetype: str | None
    version: str | None
    creation_date: str | None
    last_modification_date: str | None

class DataverseJsonSrcDataset(Dataset):
    """
    A Dataverse dataset backend that uses pre-fetched JSON content.
    """

    def __init__(self, url: str, content: str) -> None:
        """
        Create a new DataverseJsonSrcDataset.

        Args:
            url: The Dataverse dataset URL (e.g., "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/...")
            content: The JSON content as a string

        Raises:
            RuntimeError: If the URL is invalid or missing required components
        """

class ZenodoJsonSrcDataset(Dataset):
    """
    A Zenodo dataset backend that uses pre-fetched JSON content.
    """

    def __init__(self, id: str, content: str) -> None:
        """
        Create a new ZenodoJsonSrcDataset.

        Args:
            id: The Zenodo dataset ID, e.g., 931707
            content: The JSON content as a string

        Raises:
            RuntimeError
        """

class HalJsonSrcDataset(Dataset):
    """
    A HAL dataset backend that uses pre-fetched JSON content.
    """

    def __init__(self, id: str, content: str) -> None:
        """
        Create a new HalJsonSrcDataset.

        Args:
            id: The HAL dataset ID, e.g., hal-02661316
            content: The JSON content as a string

        Raises:
            RuntimeError
        """

class DabarXmlSrcDataset(Dataset):
    """
    A HAL dataset backend that uses pre-fetched JSON content.
    """

    def __init__(self, id: str, content: str) -> None:
        """
        Create a new DabarXmlSrcDataset.

        Args:
            id: The DABAR dataset ID, e.g., agr:2814
            content: The JSON content as a string

        Raises:
            RuntimeError
        """

class Dataset(object):
    def download_with_validation(self, dst_dir: pathlib.Path, limit: int = 0) -> None:
        """blocking call, using rust's async runtime"""
    def crawl_file(self) -> SyncAsyncIterator[FileEntry]:
        """returns a stream that can be either sync or async iterator over `FileEntry`"""
    def crawl(self) -> SyncAsyncIterator[FileEntry | DirEntry]:
        """returns a stream that can be either sync or async iterator over `FileEntry | DirEntry`"""
    def root_url(self) -> str: ...

def resolve(url: str, /) -> Dataset:
    """returns a dataset for the given domain"""

class DOIResolver:
    def __init__(self, timeout: int = 5) -> None:
        """Create a new DOIResolver instance.

        Args:
            timeout: HTTP timeout in seconds. Defaults to 5.
        """
    def resolve(self, doi: str, follow_redirects: bool = True) -> str:
        """Resolve a single DOI to a URL.

        Args:
            doi: The DOI to resolve, e.g. '10.1000/xyz123'.
            follow_redirects: Whether to follow redirects. Defaults to True.
        """
    def resolve_many(self, dois: list[str], follow_redirects: bool = True) -> list[str]:
        """Resolve multiple DOIs to URLs.

        Args:
          dois: List of DOIs to resolve.
          follow_redirects: Whether to follow redirects. Defaults to True.
        """
