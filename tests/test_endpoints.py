import os
import pytest
from swpt_lib import endpoints


@pytest.fixture(scope='session')
def server_name():
    os.environ['SWPT_SERVER_NAME'] = 'example.com:123'


def test_no_server_name():
    with pytest.raises(Exception):
        endpoints.build_url('debtor', debtorId=1)
    with pytest.raises(Exception):
        endpoints.match_url('debtor', 'http://example.com:123/debtors/1')


def test_build_url(server_name):
    assert endpoints.build_url('debtor', debtorId=1) == 'http://example.com:123/debtors/1'
    with pytest.raises(endpoints.BuildError):
        endpoints.build_url('debtor', unknown=1)
    with pytest.raises(endpoints.BuildError):
        endpoints.build_url('creditor', debtorId=1)
    with pytest.raises(endpoints.BuildError):
        endpoints.build_url('xxx', debtorId=1)


def test_match_url(server_name):
    assert endpoints.match_url('debtor', 'http://example.com:123/debtors/1') == {'debtorId': 1}
    with pytest.raises(endpoints.MatchError):
        endpoints.match_url('debtor', 'http://example.com/debtors/1')
    with pytest.raises(endpoints.MatchError):
        endpoints.match_url('debtor', 'http://www.example.com:123/debtors/1')
    with pytest.raises(endpoints.MatchError):
        endpoints.match_url('debtor', 'https://example.com:123/debtors/1')
    with pytest.raises(endpoints.MatchError):
        endpoints.match_url('debtor', 'http://example.com:123/xxx/1')
    with pytest.raises(endpoints.MatchError):
        endpoints.match_url('creditor', 'http://example.com:123/debtors/1')
    with pytest.raises(endpoints.MatchError):
        endpoints.match_url('creditor', 'http://ex[ample.com/debtors/1')
