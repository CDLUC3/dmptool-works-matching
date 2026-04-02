"""Session-level test environment setup."""

import os

os.environ.setdefault("AWS_ENV", "dev")
os.environ.setdefault("AWS_REGION", "us-east-1")
