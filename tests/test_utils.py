import os
import pytest
from datetime import date
from flask import Flask
from swpt_lib import utils as c

MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1


def test_get_config_value():
    os.environ['K1'] = 'one'
    os.environ['K3'] = 'three'
    app = Flask(__name__)
    app.config.from_mapping({'K1': '1', 'K2': 2})
    with app.app_context():
        assert c.get_config_value('K1') == '1'
        with pytest.raises(ValueError):
            c.get_config_value('K2')
        assert c.get_config_value('K3') == 'three'
    assert c.get_config_value('K1') == 'one'
    assert c.get_config_value('K2') is None
    assert c.get_config_value('K3') == 'three'


def test_i64_to_u64():
    assert c.i64_to_u64(0) == 0
    assert c.i64_to_u64(1) == 1
    assert c.i64_to_u64(MAX_INT64) == MAX_INT64
    assert c.i64_to_u64(-1) == MAX_UINT64
    assert c.i64_to_u64(MIN_INT64) == MAX_INT64 + 1
    with pytest.raises(ValueError):
        c.i64_to_u64(MAX_INT64 + 1)
    with pytest.raises(ValueError):
        c.i64_to_u64(MIN_INT64 - 1)


def test_u64_to_i64():
    assert c.u64_to_i64(0) == 0
    assert c.u64_to_i64(1) == 1
    assert c.u64_to_i64(MAX_INT64) == MAX_INT64
    assert c.u64_to_i64(MAX_UINT64) == -1
    assert c.u64_to_i64(MAX_INT64 + 1) == MIN_INT64
    with pytest.raises(ValueError):
        c.u64_to_i64(-1)
    with pytest.raises(ValueError):
        c.u64_to_i64(MAX_UINT64 + 1)


def test_werkzeug_converter():
    from werkzeug.routing import Map, Rule
    from werkzeug.exceptions import NotFound

    m = Map([
        Rule('/debtors/<i64:debtorId>', endpoint='debtors'),
    ], converters={'i64': c.Int64Converter})
    urls = m.bind('example.com', '/')

    # Test URL match:
    assert urls.match('/debtors/0') == ('debtors', {'debtorId': 0})
    assert urls.match('/debtors/1') == ('debtors', {'debtorId': 1})
    assert urls.match('/debtors/9223372036854775807') == ('debtors', {'debtorId': 9223372036854775807})
    assert urls.match('/debtors/9223372036854775808') == ('debtors', {'debtorId': -9223372036854775808})
    assert urls.match('/debtors/18446744073709551615') == ('debtors', {'debtorId': -1})
    with pytest.raises(NotFound):
        assert urls.match('/debtors/01')
    with pytest.raises(NotFound):
        assert urls.match('/debtors/1x')
    with pytest.raises(NotFound):
        assert urls.match('/debtors/18446744073709551616')
    with pytest.raises(NotFound):
        assert urls.match('/debtors/-1')

    # Test URL build:
    assert urls.build('debtors', {'debtorId': 0}) == '/debtors/0'
    assert urls.build('debtors', {'debtorId': 1}) == '/debtors/1'
    assert urls.build('debtors', {'debtorId': 9223372036854775807}) == '/debtors/9223372036854775807'
    assert urls.build('debtors', {'debtorId': -9223372036854775808}) == '/debtors/9223372036854775808'
    with pytest.raises(ValueError):
        assert urls.build('debtors', {'debtorId': 9223372036854775808})
    with pytest.raises(ValueError):
        assert urls.build('debtors', {'debtorId': -9223372036854775809})
    with pytest.raises(ValueError):
        assert urls.build('debtors', {'debtorId': '1x'})


def test_date_to_int24():
    assert c.date_to_int24(date(2020, 1, 1)) == 0
    assert c.date_to_int24(date(2020, 1, 2)) == 1
    assert 365 * 7000 < c.date_to_int24(date(9020, 12, 31)) < 366 * 7000
