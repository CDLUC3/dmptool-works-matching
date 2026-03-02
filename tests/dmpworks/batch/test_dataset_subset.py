import json

from dmpworks.dataset_subset import load_dois, load_institutions


def test_load_institutions(tmp_path):
    """Test Institution objects are loaded correctly."""

    data = [
        {"name": "University of Science", "ror": "01234"},
        {"name": "Institute of Art", "ror": "56789"},
    ]
    f = tmp_path / "institutions.json"
    f.write_text(json.dumps(data))

    result = load_institutions(f)
    assert len(result) == 2
    assert result[0].name == "University of Science"
    assert result[0].ror == "01234"
    assert result[1].name == "Institute of Art"
    assert result[1].ror == "56789"


def test_load_dois(tmp_path):
    """Test that DOIs are loaded correctly"""

    data = ["10.1234/example.1", "10.5678/example.2"]
    f = tmp_path / "dois.json"
    f.write_text(json.dumps(data))

    result = load_dois(f)
    assert result == data
