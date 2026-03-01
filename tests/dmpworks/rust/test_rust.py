import json

from dmpworks.rust import parse_name, revert_inverted_index, strip_markup


class TestParseName:
    def test_full_name_only_latin(self):
        parsed = parse_name(raw_full="John Doe")
        assert parsed.first_initial == "J"
        assert parsed.given_name == "John"
        assert parsed.middle_initials is None
        assert parsed.middle_names is None
        assert parsed.surname == "Doe"
        assert parsed.full == "John Doe"

    def test_full_name_inverted(self):
        parsed = parse_name(raw_full="Doe, John")
        assert parsed.first_initial == "J"
        assert parsed.given_name == "John"
        assert parsed.middle_initials is None
        assert parsed.middle_names is None
        assert parsed.surname == "Doe"
        # The full name should now preserve the original input string
        assert parsed.full == "Doe, John"

    def test_complex_full_name(self):
        parsed = parse_name(raw_full="Dr. Martin Luther King Jr.")
        assert parsed.first_initial == "M"
        assert parsed.given_name == "Martin"
        assert parsed.middle_initials == "L"
        assert parsed.middle_names == "Luther"
        assert parsed.surname == "King"
        # Original string preserved instead of "Martin Luther King, Jr."
        assert parsed.full == "Dr. Martin Luther King Jr."

    def test_explicit_given_and_surname(self):
        # Directly providing parts skips human_name and stitches the full name
        parsed = parse_name(raw_given_name="John", raw_surname="Doe")
        assert parsed.first_initial == "J"
        assert parsed.given_name == "John"
        assert parsed.middle_initials is None
        assert parsed.middle_names is None
        assert parsed.surname == "Doe"
        assert parsed.full == "John Doe"

    def test_explicit_parts_with_original_full_override(self):
        # The provided raw_full should override the stitched version
        parsed = parse_name(raw_given_name="John", raw_surname="Doe", raw_full="Dr. John Doe")
        assert parsed.first_initial == "J"
        assert parsed.given_name == "John"
        assert parsed.surname == "Doe"
        assert parsed.full == "Dr. John Doe"

    def test_explicit_cjk_name(self):
        # Non-Latin names should not get a first initial
        parsed = parse_name(raw_given_name="가은", raw_surname="김")
        assert parsed.first_initial is None
        assert parsed.given_name == "가은"
        assert parsed.surname == "김"
        assert parsed.full == "가은 김"

    def test_fallback(self):
        parsed = parse_name(raw_full="sam wu")
        assert parsed.first_initial is None
        assert parsed.given_name == "sam"
        assert parsed.middle_initials is None
        assert parsed.middle_names is None
        assert parsed.surname == "wu"
        assert parsed.full == "sam wu"

    def test_none_or_empty(self):
        for val in [None, "", "   "]:
            parsed = parse_name(raw_full=val)
            assert parsed.first_initial is None
            assert parsed.given_name is None
            assert parsed.middle_initials is None
            assert parsed.middle_names is None
            assert parsed.surname is None
            assert parsed.full is None

            # Also test when all kwargs are explicitly passed as empty/None
            parsed_all = parse_name(raw_given_name=val, raw_surname=val, raw_full=val)
            assert parsed_all.full is None


class TestStripMarkup:
    def test_basic(self):
        assert strip_markup("<p>Hello</p>") == "Hello"
        assert strip_markup("Hello") == "Hello"

    def test_nested(self):
        assert strip_markup("<div><p>Hello</p></div>") == "Hello"

    def test_null_if_equals(self):
        assert strip_markup("<p>None</p>", null_if_equals=["None"]) is None
        assert strip_markup("  ", null_if_equals=[""]) is None

    def test_none(self):
        assert strip_markup(None) is None


class TestRevertInvertedIndex:
    def test_basic(self):
        data = {"The": [0], "prelims": [1], "comprise:": [2], "Half-Title": [3]}
        encoded = json.dumps(data).encode("utf-8")
        assert revert_inverted_index(encoded) == "The prelims comprise: Half-Title"

    def test_gaps(self):
        # Gaps in indices should be skipped but order preserved
        data = {"A": [0], "C": [2]}
        encoded = json.dumps(data).encode("utf-8")
        assert revert_inverted_index(encoded) == "A C"

    def test_none(self):
        assert revert_inverted_index(None) is None
        assert revert_inverted_index(b"") is None

    def test_collision_determinism(self):
        # Test case where two words have the same index.
        # We want to ensure the output is deterministic (same order every time).
        data = {"A": [0], "B": [0]}
        encoded = json.dumps(data).encode("utf-8")

        # Run multiple times to check for determinism
        results = [revert_inverted_index(encoded) for _ in range(20)]

        # All results should be identical
        first = results[0]
        assert all(r == first for r in results)

        # Also check it is one of the expected words
        assert first in ["A", "B"]
