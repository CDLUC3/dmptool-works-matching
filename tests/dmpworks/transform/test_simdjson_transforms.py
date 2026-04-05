import unicodedata

import pytest

from dmpworks.transform.simdjson_transforms import extract_doi


class TestExtractDoi:
    @pytest.mark.parametrize(
        "input_text,expected",
        [
            ("10.1234/abc", "10.1234/abc"),
            ("doi: 10.1234/ABC.def", "10.1234/abc.def"),
            (None, None),
            ("no doi here", None),
        ],
    )
    def test_basic_extraction(self, input_text, expected):
        assert extract_doi(input_text) == expected

    def test_nfd_input_normalized_to_nfc(self):
        """NFD decomposed o + combining accent is normalized to NFC precomposed."""
        nfd_doi = "10.26767/colo\u0301quio.v17i4.1817"
        result = extract_doi(nfd_doi)
        assert result == "10.26767/col\u00f3quio.v17i4.1817"
        assert unicodedata.is_normalized("NFC", result)

    def test_nfc_input_unchanged(self):
        """Already NFC input passes through unchanged."""
        nfc_doi = "10.26767/col\u00f3quio.v17i4.1817"
        result = extract_doi(nfc_doi)
        assert result == nfc_doi
        assert unicodedata.is_normalized("NFC", result)
