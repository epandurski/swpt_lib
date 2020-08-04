import re
import binascii
from base64 import urlsafe_b64encode, urlsafe_b64decode
from typing import Tuple
from swpt_lib.utils import u64_to_i64, i64_to_u64

PREFIX = r'^swpt:(\d{1,20})'
URLSAFE_B64 = re.compile(r'^[A-Za-z0-9_=-]*$')
SWPT_DEBTOR_URI = re.compile(PREFIX + '$')
SWPT_ACCOUNT_URI = re.compile(PREFIX + r'/(!?[A-Za-z0-9_=-]{1,136})$')


def parse_debtor_uri(uri: str) -> int:
    """Return a debtor ID.

    Raises `ValueError` if the passed URI does not represent a valid
    SWPT debtor.

    """

    m = SWPT_DEBTOR_URI.match(uri)
    if m is None:
        raise ValueError()
    return u64_to_i64(int(m[1]))


def parse_account_uri(uri: str) -> Tuple[int, str]:
    """Return a (debtor ID, account ID) tuple.

    Raises `ValueError` if the passed URI does not represent a valid
    SWPT account.

    """

    m = SWPT_ACCOUNT_URI.match(uri)
    if m is None:
        raise ValueError()
    debtor_id = u64_to_i64(int(m[1]))
    account_id = m[2]

    if account_id[0] == '!':
        encoded_account_id = account_id[1:].encode('ascii')
        try:
            account_id = urlsafe_b64decode(encoded_account_id)

            # Make sure this is the canonical encoding.
            if urlsafe_b64encode(account_id) != encoded_account_id:
                raise ValueError

            account_id = account_id.decode('ascii')

        except (binascii.Error, UnicodeDecodeError):
            raise ValueError from None

    if len(account_id) > 100:
        raise ValueError

    assert isinstance(debtor_id, int)
    assert isinstance(account_id, str)
    return debtor_id, account_id


def make_debtor_uri(debtor_id: int) -> str:
    """Return a valid SWPT debtor URI.

    Raises `ValueError` if the passed `debtor_id` is invalid.

    """

    return f'swpt:{i64_to_u64(debtor_id)}'


def make_account_uri(debtor_id: int, account_id: str) -> str:
    """Return a valid SWPT account URI.

    Raises `ValueError` if the passed `debtor_id` or `account_id` is
    invalid.

    """

    if not 1 <= len(account_id) <= 100:
        raise ValueError

    if not URLSAFE_B64.match(account_id):
        try:
            ascii_encoded = account_id.encode('ascii')
        except UnicodeEncodeError:
            raise ValueError from None

        b64_encoded = urlsafe_b64encode(ascii_encoded)
        account_id = f'!{b64_encoded.decode()}'

    return f'swpt:{i64_to_u64(debtor_id)}/{account_id}'
