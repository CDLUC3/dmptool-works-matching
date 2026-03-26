import pytest

SETUP_LOGGING = "dmpworks.utils.setup_multiprocessing_logging"


@pytest.fixture(autouse=True)
def mock_setup_logging(mocker):
    """Suppress multiprocessing logging setup in all batch CLI tests."""
    return mocker.patch(SETUP_LOGGING)
