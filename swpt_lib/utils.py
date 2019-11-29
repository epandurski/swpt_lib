from werkzeug.routing import BaseConverter, ValidationError

_MIN_INT64 = -1 << 63
_MAX_INT64 = (1 << 63) - 1
_MAX_UINT64 = (1 << 64) - 1
_I64_SPAN = _MAX_UINT64 + 1


def i64_to_u64(value: int) -> int:
    """"Convert a signed 64-bit integer to unsigned 64-bit integer.

    Raises `ValueError` if the value is not in the range of signed
    64-bit integers.

    """

    if value > _MAX_INT64 or value < _MIN_INT64:
        raise ValueError()
    if value >= 0:
        return value
    return value + _I64_SPAN


def u64_to_i64(value: int) -> int:
    """"Convert an unsigned 64-bit integer to a signed 64-bit integer.

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
