import datetime

import pendulum
import pytest

from dmpworks.model.common import (
    parse_pendulum_date,
    parse_pendulum_datetime,
    serialize_pendulum_date,
    serialize_pendulum_datetime,
)


class TestParsePendulumDatetime:
    def test_returns_none(self):
        assert parse_pendulum_datetime(None) is None

    def test_handles_pendulum_datetime(self):
        dt = pendulum.datetime(2026, 3, 4, 12, 0, 0)
        result = parse_pendulum_datetime(dt)
        assert type(result) is pendulum.DateTime
        assert result == dt

    def test_converts_standard_datetime(self):
        dt = datetime.datetime(2026, 3, 4, 12, 0, 0)
        result = parse_pendulum_datetime(dt)
        assert type(result) is pendulum.DateTime
        assert result.year == 2026

    def test_parses_valid_datetime_string(self):
        result = parse_pendulum_datetime("2026-03-04T12:00:00Z")
        assert type(result) is pendulum.DateTime
        assert result.year == 2026

    def test_parses_date_string_to_midnight_datetime(self):
        result = parse_pendulum_datetime("2026-03-04")
        assert type(result) is pendulum.DateTime
        assert result.hour == 0 and result.minute == 0

    def test_raises_value_error_on_invalid_string(self):
        with pytest.raises(ValueError, match="Failed to parse datetime string"):
            parse_pendulum_datetime("not-a-date")

    def test_converts_date_objects_to_midnight_datetime(self):
        std_d = datetime.date(2026, 3, 4)
        pen_d = pendulum.date(2026, 3, 4)

        for val in (std_d, pen_d):
            result = parse_pendulum_datetime(val)

            assert type(result) is pendulum.DateTime

            assert result.year == 2026
            assert result.month == 3
            assert result.day == 4
            assert result.hour == 0
            assert result.minute == 0
            assert result.second == 0

    def test_raises_type_error_on_invalid_type(self):
        with pytest.raises(TypeError, match="Expected str"):
            parse_pendulum_datetime(12345)


class TestSerializePendulumDatetime:
    def test_returns_none(self):
        assert serialize_pendulum_datetime(None) is None

    def test_serializes_pendulum_datetime(self):
        dt = pendulum.datetime(2026, 3, 4, 12, 0, 0, tz="UTC")
        result = serialize_pendulum_datetime(dt)
        assert type(result) is str
        assert result == "2026-03-04T12:00:00Z"

    def test_raises_type_error_on_invalid_type(self):
        with pytest.raises(TypeError, match="Expected pendulum.DateTime"):
            serialize_pendulum_datetime("2026-03-04")


class TestParsePendulumDate:
    def test_returns_none(self):
        assert parse_pendulum_date(None) is None

    def test_handles_pendulum_date(self):
        d = pendulum.date(2026, 3, 4)
        result = parse_pendulum_date(d)
        assert type(result) is pendulum.Date
        assert type(result) is not pendulum.DateTime
        assert result == d

    def test_converts_datetime_objects(self):
        std_dt = datetime.datetime(2026, 3, 4, 12, 0, 0)
        pen_dt = pendulum.datetime(2026, 3, 4, 12, 0, 0)
        std_d = datetime.date(2026, 3, 4)

        for val in (std_dt, pen_dt, std_d):
            result = parse_pendulum_date(val)
            assert type(result) is pendulum.Date
            assert type(result) is not pendulum.DateTime
            assert result == pendulum.date(2026, 3, 4)

    def test_parses_valid_strings(self):
        for val in ("2026-03-04", "2026-03-04T12:00:00Z"):
            result = parse_pendulum_date(val)
            assert type(result) is pendulum.Date
            assert type(result) is not pendulum.DateTime
            assert result == pendulum.date(2026, 3, 4)

    def test_strictly_converts_datetime_to_date_and_allows_comparison(self):
        # Specific regression test for the OpenSearch query issue
        pen_dt = pendulum.datetime(2026, 3, 4, 12, 30, 0)
        result = parse_pendulum_date(pen_dt)

        assert type(result) is pendulum.Date
        assert type(result) is not pendulum.DateTime

        baseline_date = pendulum.date(1990, 1, 1)
        try:
            is_valid = result >= baseline_date
            assert is_valid is True
        except TypeError:
            pytest.fail("Result behaved like a DateTime and raised a TypeError during comparison.")

    def test_raises_value_error_on_invalid_string(self):
        with pytest.raises(ValueError, match="Failed to parse date string"):
            parse_pendulum_date("invalid-date-string")

    def test_raises_type_error_on_invalid_type(self):
        with pytest.raises(TypeError, match="Expected str"):
            parse_pendulum_date(12345)


class TestSerializePendulumDate:
    def test_returns_none(self):
        assert serialize_pendulum_date(None) is None

    def test_serializes_pendulum_date(self):
        d = pendulum.date(2026, 3, 4)
        result = serialize_pendulum_date(d)
        assert type(result) is str
        assert result == "2026-03-04"

    def test_raises_type_error_on_invalid_type(self):
        with pytest.raises(TypeError, match="Expected pendulum.Date"):
            serialize_pendulum_date(datetime.date(2026, 3, 4))
