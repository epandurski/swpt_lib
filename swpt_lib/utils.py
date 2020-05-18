from __future__ import annotations
import os
from functools import total_ordering
from datetime import date, datetime, timedelta
from typing import Optional, Tuple
from werkzeug.routing import BaseConverter, ValidationError
from flask import current_app

_MIN_INT32 = -1 << 31
_MAX_INT32 = (1 << 31) - 1
_MIN_INT64 = -1 << 63
_MAX_INT64 = (1 << 63) - 1
_MAX_UINT64 = (1 << 64) - 1
_I64_SPAN = _MAX_UINT64 + 1
_DATE_1970_01_01 = date(1970, 1, 1)
_TD_PLUS_SECOND = timedelta(seconds=1)
_TD_MINUS_SECOND = timedelta(seconds=-1)


class _MISSING:
    pass


@total_ordering
class Seqnum:
    """A signed 32-bit integer seqnum value.

    Comparisions beteen `Seqnum` instances correctly deals with the
    possible 32-bit integer wrapping.

    """

    def __init__(self, value: int):
        assert _MIN_INT32 <= value <= _MAX_INT32
        self.value = value

    def __eq__(self, other: Seqnum):
        return self.value == other.value

    def __gt__(self, other: Seqnum):
        return 0 < (self.value - other.value) % 0x100000000 < 0x80000000

    def increment(self) -> Seqnum:
        """Return an incremented `Seqnum` instance."""

        value = self.value
        assert _MIN_INT32 <= value <= _MAX_INT32
        return Seqnum(_MIN_INT32 if value == _MAX_INT32 else value + 1)


def get_config_value(key: str) -> Optional[str]:
    """Get the value for the configuration variable with a name `key`.

    The returned value is either a string or `None`. If there is a
    `Flask` application context, the app's config will be checked
    first. If that fails, the environment will be checked next. If
    that fails too, `None` will be returned.

    """

    app_config_value = current_app.config.get(key, _MISSING) if current_app else _MISSING
    if app_config_value is _MISSING:
        return os.environ.get(key)
    if not isinstance(app_config_value, str):
        raise ValueError(f'a non-string value for "{key}"')
    return app_config_value


def i64_to_u64(value: int) -> int:
    """Convert a signed 64-bit integer to unsigned 64-bit integer.

    Raises `ValueError` if the value is not in the range of signed
    64-bit integers.

    """

    if value > _MAX_INT64 or value < _MIN_INT64:
        raise ValueError()
    if value >= 0:
        return value
    return value + _I64_SPAN


def u64_to_i64(value: int) -> int:
    """Convert an unsigned 64-bit integer to a signed 64-bit integer.

    Raises `ValueError` if the value is not in the range of unsigned
    64-bit integers.

    """

    if value > _MAX_UINT64 or value < 0:
        raise ValueError()
    if value <= _MAX_INT64:
        return value
    return value - _I64_SPAN


class Int64Converter(BaseConverter):
    """Flask URL converter for signed 64-bit integers.

    The converter can be registered with the Flask app like this::

      from flask import Flask
      from swpt_lib.utils import Int64Converter

      app = Flask(__name__)
      app.url_map.converters['i64'] = Int64Converter

    """

    regex = r"0|[1-9][0-9]{0,19}"

    def to_python(self, value):
        try:
            return u64_to_i64(int(value))
        except ValueError:
            raise ValidationError()

    def to_url(self, value):
        value = int(value)
        return str(i64_to_u64(value))


def date_to_int24(d: date) -> int:
    """Return a non-negative 24-bit integer derived from a date.

    The passed date must not be before January 1st, 1970. The returned
    integer equals the number of days passed since January 1st, 1970.

    """

    days = (d - _DATE_1970_01_01).days
    assert days >= 0
    assert days >> 24 == 0
    return days


def is_later_event(event: Tuple[datetime, int], other_event: Tuple[Optional[datetime], Optional[int]]) -> bool:
    """Return whether `event` is later than `other_event`.

    Each of the passed events must be a (`datetime`, `int`) tuple. The
    `datetime` must be the event timestamp, and the `int` must be the
    event sequential number (32-bit signed integer, with eventual
    wrapping).

    An event with a noticeably later timestamp (>= 1s) is always
    considered later than an event with an earlier timestamp. Only
    when the two timestamps are very close (< 1s), the sequential
    numbers of the events are compared. When the timestamp of
    `other_event` is `None`, `event` is considered as a later event.

    Note that sequential numbers are compared with possible 32-bit
    signed integer wrapping in mind. For example, compared to
    2147483647, -21474836478 is considered a later sequential number.

    """

    ts, seqnum = event
    other_ts, other_seqnum = other_event
    if other_ts is None:
        return True
    advance = ts - other_ts
    if advance >= _TD_PLUS_SECOND:
        return True
    if advance <= _TD_MINUS_SECOND:
        return False
    return other_seqnum is None or 0 < (seqnum - other_seqnum) % 0x100000000 < 0x80000000


def increment_seqnum(n: int) -> int:
    """Increment a 32-bit signed integer with wrapping."""

    assert _MIN_INT32 <= n <= _MAX_INT32
    return _MIN_INT32 if n == _MAX_INT32 else n + 1
