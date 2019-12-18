import logging
import os
from datetime import timedelta
import pytest
from sqlalchemy import MetaData, Table, Column, Integer, String, create_engine
from swpt_lib.scan_table import _TableReader, TableScanner


@pytest.fixture(scope='session')
def user_table():
    metadata = MetaData()
    user_table = Table(
        'user_table', metadata,
        Column('user_id', Integer, primary_key=True),
        Column('user_name', String(16), nullable=False),
        Column('email_address', String(60)),
    )
    return user_table


@pytest.fixture(scope='session')
def engine(user_table):
    engine = create_engine(os.environ['SQLALCHEMY_DATABASE_URI'])
    user_table.metadata.drop_all(engine)
    user_table.metadata.create_all(engine)
    for i in range(10):
        stmt = user_table.insert().values(user_id=i, user_name=f'user_{i}', email_address='user_{i}@example.com')
        engine.execute(stmt)
    engine.execute('ANALYZE user_table')
    return engine


def test_table_reader(user_table, engine, caplog):
    caplog.set_level(logging.INFO)
    reader = _TableReader('TestReader', engine, user_table, 40, [user_table.c.user_id, user_table.c.user_name])
    rows = []
    while len(rows) < 100:
        rows.extend(reader.read_rows(1000))
    assert len({r['user_id'] for r in rows}) == 10
    assert hasattr(rows[0], 'user_name')
    assert not hasattr(rows[0], 'email_address')
    assert 'TestReader reached the end of the table' in caplog.text


def test_user_scanner(user_table, engine):
    class ProcessError(Exception):
        pass

    class UserScanner(TableScanner):
        table = user_table
        columns = [table.c.user_id, table.c.user_name]

        def __init__(self):
            self.count = 0

        def process_rows(self, rows):
            self.count += len(rows)
            if self.count > 30:
                raise ProcessError()

    scanner = UserScanner()
    with pytest.raises(ProcessError):
        scanner.run(engine, timedelta(seconds=0.5))
    assert scanner.count > 30
