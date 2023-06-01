from unittest import mock

import pytest

from b2sdk.raw_api import REALM_URLS


@pytest.fixture(autouse=True, scope='session')
def mock_realm_urls():
    with mock.patch.dict(REALM_URLS, {'production': 'http://production.example.com'}):
        yield
