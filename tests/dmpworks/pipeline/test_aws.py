"""Tests for pipeline AWS helper functions."""

import pytest

from dmpworks.pipeline.aws import get_bucket_name, get_eventbridge_rule_name, get_state_machine_arn


class TestGetBucketName:
    @pytest.mark.parametrize(
        "env,expected",
        [
            ("dev", "dmpworks-dev-s3"),
            ("stg", "dmpworks-stg-s3"),
            ("prd", "dmpworks-prd-s3"),
        ],
        ids=["dev", "stg", "prd"],
    )
    def test_returns_convention_based_name(self, env, expected):
        print(f"env={env}")
        assert get_bucket_name(env=env) == expected
