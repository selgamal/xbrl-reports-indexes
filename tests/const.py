"""gets path of test data dir for tests"""
from __future__ import annotations

import pathlib

from xbrlreportsindexes.core import constants


def test_data_dir() -> pathlib.Path:
    """Returns the path to mock data dir"""
    this_dir = pathlib.Path(__file__).parent
    test_mock_data_dir: pathlib.Path = this_dir.joinpath(
        constants.MOCK_TEST_DATA_DIR_NAME
    )
    return test_mock_data_dir
