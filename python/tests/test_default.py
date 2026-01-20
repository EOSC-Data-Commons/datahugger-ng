from pathlib import Path
from datahugger import resolve


def test_resolve_default():
    record = resolve(
        "https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/KBHLOD"
    )

    assert record.id() == "doi:10.7910/DVN/KBHLOD"
    assert (
        record.root_url()
        == "https://dataverse.harvard.edu/api/datasets/:persistentId/versions/:latest-published?persistentId=doi%3A10.7910%2FDVN%2FKBHLOD"
    )


def test_download(tmp_path: Path):
    """real call to download, can be not stable. Since it is only for the non-recommended API,
    this test is acceptable.
    """
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
