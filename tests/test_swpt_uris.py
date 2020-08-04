import pytest
from swpt_lib.swpt_uris import parse_debtor_uri, parse_account_uri, make_debtor_uri, make_account_uri


def test_parse_debtor_uri():
    assert parse_debtor_uri('swpt:0') == 0
    assert parse_debtor_uri('swpt:1') == 1
    assert parse_debtor_uri('swpt:2') == 2
    assert parse_debtor_uri('swpt:9223372036854775807') == 9223372036854775807
    assert parse_debtor_uri('swpt:9223372036854775808') == -9223372036854775808
    assert parse_debtor_uri('swpt:18446744073709551615') == -1

    for uri in [
            'SWPT:1',
            ' swpt:1',
            'swpt:1 ',
            'swpt:',
            'swpt:-1',
            'swpt:18446744073709551616',
    ]:
        with pytest.raises(ValueError):
            parse_debtor_uri(uri)


def test_parse_account_uri():
    A100 = 100 * 'A'
    A132 = 132 * 'A'
    assert parse_account_uri('swpt:1/1') == (1, '1')
    assert parse_account_uri('swpt:1/abc-_=123') == (1, 'abc-_=123')
    assert parse_account_uri('swpt:1/!aA==') == (1, 'h')
    assert parse_account_uri(f'swpt:1/{A100}') == (1, A100)
    assert parse_account_uri(f'swpt:1/!{A132}AA==') == (1, 100 * '\0')

    for uri in [
            'SWPT:1/',
            ' swpt:1/1',
            'swpt:1/1 ',
            'swpt:',
            'swpt:-1/1',
            'swpt:18446744073709551616/1',
            'swpt:1/!a=A==',
            'swpt:1/!9A==',
            f'swpt:1/{A100}A',
            f'swpt:1/!{A132}A',
            f'swpt:1/!{A132}AA',
            f'swpt:1/!{A132}AAAA',
            f'swpt:1/!{A132}AAAAA',
    ]:
        with pytest.raises(ValueError):
            parse_account_uri(uri)


def test_make_debtor_uri():
    assert make_debtor_uri(0) == 'swpt:0'
    assert make_debtor_uri(1) == 'swpt:1'
    assert make_debtor_uri(9223372036854775807) == 'swpt:9223372036854775807'
    assert make_debtor_uri(-9223372036854775808) == 'swpt:9223372036854775808'
    assert make_debtor_uri(-1) == 'swpt:18446744073709551615'

    with pytest.raises(ValueError):
        make_debtor_uri(9223372036854775808)

    with pytest.raises(ValueError):
        make_debtor_uri(-9223372036854775809)


def test_make_account_uri():
    correct = [
        (0, 'abc'),
        (1, 100 * 'A'),
        (9223372036854775807, '%$@%Dfda-'),
        (-9223372036854775808, '!%$@%Dfda-'),
        (-1, '\n!\t?'),
    ]

    for t in correct:
        assert parse_account_uri(make_account_uri(*t)) == t

    incorrect = [
        (9223372036854775808, 'abc'),
        (-9223372036854775809, 'abc'),
        (1, 101 * 'A'),
        (1, ''),
        (1, '\xc0'),
    ]

    for t in incorrect:
        with pytest.raises(ValueError):
            make_account_uri(*t)
