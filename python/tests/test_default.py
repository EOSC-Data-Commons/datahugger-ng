import asyncio
import pathlib
import pytest
from pathlib import Path
from datahugger import (
    FileEntry,
    resolve,
    DOIResolver,
    DataverseJsonSrcDataset,
    ZenodoJsonSrcDataset,
    HalJsonSrcDataset,
    DabarXmlSrcDataset,
)
import requests


def test_resolve_default():
    ds = resolve(
        "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD"
    )

    assert (
        ds.root_url()
        == "https://dataverse.harvard.edu/api/datasets/:persistentId/versions/:latest-published?persistentId=doi%3A10.7910%2FDVN%2FKBHLOD"
    )


def test_resolve_doi_blocking():
    doi_resolver = DOIResolver(timeout=30)

    url = doi_resolver.resolve("10.34894/0B7ZLK", False)
    assert url == "https://dataverse.nl/citation?persistentId=doi:10.34894/0B7ZLK"

    url = doi_resolver.resolve("10.34894/0B7ZLK")
    assert url == "https://dataverse.nl/dataset.xhtml?persistentId=doi:10.34894/0B7ZLK"

    urls = doi_resolver.resolve_many(
        ["10.34894/0B7ZLK", "10.17026/DANS-2AC-ETD6", "10.17026/DANS-2BA-UAVX"], False
    )

    assert urls == [
        "https://dataverse.nl/citation?persistentId=doi:10.34894/0B7ZLK",
        "https://phys-techsciences.datastations.nl/citation?persistentId=doi:10.17026/DANS-2AC-ETD6",
        "https://phys-techsciences.datastations.nl/citation?persistentId=doi:10.17026/DANS-2BA-UAVX",
    ]

    urls = doi_resolver.resolve_many(
        ["10.34894/0B7ZLK", "10.17026/DANS-2AC-ETD6", "10.17026/DANS-2BA-UAVX"]
    )

    assert urls == [
        "https://dataverse.nl/dataset.xhtml?persistentId=doi:10.34894/0B7ZLK",
        "https://phys-techsciences.datastations.nl/dataset.xhtml?persistentId=doi:10.17026/DANS-2AC-ETD6",
        "https://phys-techsciences.datastations.nl/dataset.xhtml?persistentId=doi:10.17026/DANS-2BA-UAVX",
    ]


def test_download(tmp_path: Path) -> None:
    """real call to download, can be not stable. Since it is only for the non-recommended API,
    this test is acceptable.
    """
    ds = resolve(
        "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD"
    )
    ds.download_with_validation(tmp_path)
    assert sorted([i.name for i in tmp_path.iterdir()]) == [
        "ECM_matrix.py",
        "Markov_comp.py",
        "Markov_learning.py",
        "tutorial1.py",
        "tutorial2.py",
        "tutorial3.py",
        "tutorial4.py",
    ]


def test_dataclass_constructor():
    entry = FileEntry(
        None,
        None,
        pathlib.Path("/tmp/x"),
        "https://example.com/download_url",
        None,
        [],
        None,
        None,
        None,
        None,
    )
    assert str(entry.path_crawl_rel.as_posix()) == "/tmp/x"
    assert entry.download_url == "https://example.com/download_url"
    assert entry.size is None
    assert entry.checksum == []

    entry.size = 12
    assert entry.size == 12


def test_crawl_blocking():
    ds = resolve(
        "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD"
    )
    for i in ds.crawl():
        print(i)

    for i in ds.crawl_file():
        print(i)


def test_dataverse_from_json():
    try:
        response = requests.get(
            "https://dataverse.harvard.edu/api/datasets/:persistentId/versions/:latest-published?persistentId=doi:10.7910/DVN/KBHLOD",
            timeout=60,
        )
        response.raise_for_status()
        dataverse = response.text

    except Exception as e:
        print("fetching JSON failed")
        raise e

    ds = DataverseJsonSrcDataset(
        "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD",
        dataverse,
    )

    for i in ds.crawl_file():
        print(i)


def test_zenodo_from_json():
    try:
        response = requests.get(
            "https://zenodo.org/api/records/19109278/files",
            timeout=60,
        )
        response.raise_for_status()
        zenodo = response.text

    except Exception as e:
        print("fetching JSON failed")
        raise e

    ds = ZenodoJsonSrcDataset(
        "19109278",
        zenodo,
    )

    for i in ds.crawl_file():
        print(i)


def test_hal_from_json():
    try:
        response = requests.get(
            "https://api.archives-ouvertes.fr/search/?q=halId_s:hal-02661316&wt=json&fl=halId_s,fileMain_s,files_s,fileType_s,modifiedDate_tdate,producedDate_tdate,version_i",
            timeout=60,
        )
        response.raise_for_status()
        hal = response.text

    except Exception as e:
        print("fetching JSON failed")
        raise e

    ds = HalJsonSrcDataset(
        "hal-02661316",
        hal,
    )

    for i in ds.crawl_file():
        print(i)


def test_dabar_from_xml1():
    try:
        response = requests.get(
            "https://dabar.srce.hr/oai/?verb=GetRecord&metadataPrefix=mods&identifier=oai:dabar.srce.hr:biotechri_715",
            timeout=60,
        )
        response.raise_for_status()
        dabar = response.text

    except Exception as e:
        print("fetching JSON failed")
        raise e

    ds = DabarXmlSrcDataset("", dabar)

    for i in ds.crawl_file():
        print(i)


def test_dabar_from_xml2():
    try:
        response = requests.get(
            "https://data.fulir.irb.hr/oai/?verb=GetRecord&metadataPrefix=mods&identifier=oai:data.fulir.irb.hr:irb_106",
            timeout=60,
        )
        response.raise_for_status()
        dabar = response.text

    except Exception as e:
        print("fetching JSON failed")
        raise e

    ds = DabarXmlSrcDataset("", dabar)

    for i in ds.crawl_file():
        print(i)


@pytest.mark.asyncio
async def test_crawl_async():
    """not rigrous test but the async is happenning that clock ticks before crawling complete."""
    ds = resolve(
        "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD"
    )

    async def do_other_work():
        for _ in range(5):
            print("tick")
            await asyncio.sleep(0.1)

    async def crawl_task():
        async for i in ds.crawl_file():
            print("crawl:", i)

    # run both concurrently
    _ = await asyncio.gather(crawl_task(), do_other_work())
