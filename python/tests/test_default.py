import asyncio
import pytest
from pathlib import Path
from datahugger import resolve


def test_resolve_default():
    ds = resolve(
        "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD"
    )

    assert (
        ds.root_url()
        == "https://dataverse.harvard.edu/api/datasets/:persistentId/versions/:latest-published?persistentId=doi%3A10.7910%2FDVN%2FKBHLOD"
    )


def test_download(tmp_path: Path):
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


def test_crawl_blocking():
    ds = resolve(
        "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD"
    )
    for i in ds.crawl():
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
        async for i in ds.crawl():
            print("crawl:", i)

    # run both concurrently
    _ = await asyncio.gather(crawl_task(), do_other_work())
