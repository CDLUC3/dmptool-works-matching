import json

from dmpworks.dataset_subset import load_dois, load_institutions


def test_load_institutions(tmp_path):
    """Test Institution objects are loaded correctly, with ROR IDs normalised via extract_ror."""
    data = [
        {"name": "University of Science", "ror": "https://ror.org/01an7q238"},
        {"name": "Institute of Art", "ror": "02mhbyk09"},
        {"name": "No ROR Institution", "ror": None},
        {"name": None, "ror": None},  # should be excluded
    ]
    f = tmp_path / "institutions.json"
    f.write_text(json.dumps(data))

    result = load_institutions(f)
    assert len(result) == 3
    assert result[0].name == "University of Science"
    assert result[0].ror == "01an7q238"
    assert result[1].name == "Institute of Art"
    assert result[1].ror == "02mhbyk09"
    assert result[2].name == "No ROR Institution"
    assert result[2].ror is None


def test_load_dois(tmp_path):
    """Test that DOIs are loaded and normalised via extract_doi, with invalid entries excluded."""
    data = ["10.1234/example.1", "https://doi.org/10.5678/example.2", "not-a-doi"]
    f = tmp_path / "dois.json"
    f.write_text(json.dumps(data))

    result = load_dois(f)
    assert result == ["10.1234/example.1", "10.5678/example.2"]
