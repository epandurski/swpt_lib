from typing import NamedTuple, List
from collections import deque
import sqlalchemy
from sqlalchemy.schema import Table
from sqlalchemy.engine import Connectable
from sqlalchemy.sql.expression import ColumnElement
from datetime import timedelta, datetime, timezone
from math import ceil
import time
import random

__all__ = ['TableScanner', 'DEFAULT_BLOCKS_PER_QUERY', 'DEFAULT_TARGET_BEAT_DURATION']

DEFAULT_BLOCKS_PER_QUERY = 40
"""The default number of blocks to be retrieved with one query."""

DEFAULT_TARGET_BEAT_DURATION = 25
"""The default target duration of scanning beats in milliseconds."""


class EndOfTableError(Exception):
    """The end of the table has been reached."""


class TableReader:
    """Reads a table sequentially, ad infinitum."""

    LAST_BLOCK_QUERY = """SELECT pg_relation_size('{tablename}') / current_setting('block_size')::int"""

    def __init__(self, engine: Connectable, table: Table, blocks_per_query: int, columns: List[ColumnElement] = None):
        assert blocks_per_query >= 1
        self.engine = engine
        self.table = table
        self.table_query = sqlalchemy.select(columns or table.columns)
        self.blocks_per_query = blocks_per_query
        self.current_block = -1
        self.queue = deque()

    def _ensure_valid_current_block(self):
        last_block = self.engine.execute(self.LAST_BLOCK_QUERY.format(tablename=self.table.name))
        total_blocks = last_block.scalar() + 1
        assert total_blocks > 0
        if self.current_block < 0:
            self.current_block = random.randrange(total_blocks)
        if self.current_block >= total_blocks:
            raise EndOfTableError()

    def _advance_current_block(self) -> list:
        self._ensure_valid_current_block()
        first_block = self.current_block
        self.current_block += self.blocks_per_query
        last_block = self.current_block - 1
        tid_range_clause = sqlalchemy.text(f"""ctid = ANY (ARRAY (
          SELECT ('(' || b.b || ',' || t.t || ')')::tid
          FROM generate_series({first_block:d}, {last_block:d}) AS b(b),
               generate_series(0, current_setting('block_size')::int / 32) AS t(t)
        ))
        """)
        tid_range_query = self.table_query.where(tid_range_clause)
        return self.engine.execute(tid_range_query).fetchall()

    def read_rows(self, count) -> list:
        """Return a list of at most `count` rows."""

        rows = self.queue
        while len(rows) < count:
            try:
                rows.extend(self._advance_current_block())
            except EndOfTableError:
                self.current_block = 0
                break
        return [rows.pop() for _ in range(count)]


class RhythmParams(NamedTuple):
    rows_per_beat: int
    beat_duration: timedelta
    last_beat_ended_at: datetime
    reset_at: datetime
    saved_time: timedelta


class TableScanner:
    """A table-scanner super-class. Each subclass can define the following
    attributes:

      `table`

         The :class:`~sqlalchemy.schema.Table` that will be scanned
         (`model.__table__` if declarative base is used). **Must be
         defined in the subclass.**

      `columns`

         An optional list of
         :class:`~sqlalchemy.sql.expression.ColumnElement` instances
         to be be retrieved for each row. Most of the time it will be
         a list of :class:`~sqlalchemy.schema.Column`
         instances. Defaults to `table.columns`.

      `blocks_per_query`

         The number of database pages (blocks) to be retrieved with
         one query. Defaults to ``DEFAULT_BLOCKS_PER_QUERY``. It might
         be a good idea to increase this number when the size of the
         table row is big.

      `target_beat_duration`

         The scanning of the table is done in a sequence of
         "beats". This attribute determines the ideal duration in
         milliseconds of those beats. The value should be big enough
         so that, on average, the operations performed on table's rows
         could be completed within this interval. Setting this value
         too high may have the effect of too many rows being processed
         simultaneously in one beat. The default value is
         ``DEFAULT_TARGET_BEAT_DURATION``.

    :param engine: SQLAlchemy engine

    :param completion_goal: The time interval in which the whole table
      should be processed. This is merely an approximate goal. In
      reality, scans can take any amount of time.

    Exapmle::

      from swpt_lib.scan_table import TableScanner
      from mymodels import Customer

      class CustomerScanner(TableScanner):
          table = Customer.__table__
          columns = [Customer.id, Customer.last_order_date]

          def process_rows(self, rows):
              for row in rows:
                  print(row['id'], row['last_order_date'])

    """

    TD_ZERO = timedelta(seconds=0)
    TD_MIN_SLEEPTIME = timedelta(milliseconds=10)
    TOTAL_ROWS_QUERY = """SELECT reltuples::bigint FROM pg_catalog.pg_class WHERE relname = '{tablename}'"""

    columns = None
    blocks_per_query = DEFAULT_BLOCKS_PER_QUERY
    target_beat_duration = DEFAULT_TARGET_BEAT_DURATION

    def __init__(self, engine: Connectable, completion_goal: timedelta):
        assert hasattr(self, 'table'), '"table" must be defined in the subclass.'
        self.__engine = engine
        self.__completion_goal = completion_goal
        self.__reader = TableReader(engine, self.table, self.blocks_per_query, self.columns)
        self.__rhythm = None

    def __set_rhythm(self) -> None:
        completion_goal = self.__completion_goal
        assert completion_goal > self.TD_ZERO
        target_beat_duration = timedelta(milliseconds=self.target_beat_duration)
        assert target_beat_duration > self.TD_ZERO
        target_number_of_beats = max(1, completion_goal // target_beat_duration)
        total_rows = self.__engine.execute(self.TOTAL_ROWS_QUERY.format(tablename=self.table.name)).scalar()
        assert total_rows >= 0
        rows_per_beat = max(1, ceil(total_rows / target_number_of_beats))
        number_of_beats = max(1, ceil(total_rows / rows_per_beat))
        current_ts = datetime.now(tz=timezone.utc)
        self.__rhythm = RhythmParams(
            rows_per_beat=rows_per_beat,
            beat_duration=completion_goal / number_of_beats,
            last_beat_ended_at=current_ts,
            reset_at=current_ts + completion_goal,
            saved_time=self.TD_ZERO,
        )

    def __calc_elapsed_time(self) -> timedelta:
        rhythm = self.__rhythm
        current_ts = datetime.now(tz=timezone.utc)
        elapsed_time = current_ts - rhythm.last_beat_ended_at
        rhythm.last_beat_ended_at = current_ts
        return elapsed_time

    def __beat(self) -> None:
        rhythm = self.__rhythm
        rows = self.__reader.read_rows(count=rhythm.rows_per_beat)
        self.process_rows(rows)
        rhythm.saved_time += rhythm.beat_duration - self.__calc_elapsed_time()
        if rhythm.saved_time > self.TD_MIN_SLEEPTIME:
            time.sleep(rhythm.saved_time.total_seconds())
            rhythm.saved_time -= self.__calc_elapsed_time()

    def run(self):
        """Scan table continuously.

        The table is scanned sequentially, starting from a random
        row. During the scan :meth:`process_rows` will be continuously
        invoked with a list of rows. When the end of the table is
        reached, the scan continues from the beginning, ad infinitum.

        """

        rhythm = self.__rhythm
        while True:
            self.__set_rhythm()
            while rhythm.last_beat_ended_at < rhythm.reset_at:
                self.__beat()

    def process_rows(self, rows: list) -> None:
        """Process a list or rows.

        **Must be defined in the subclass.**

        :param rows: A list of table rows

        """

        raise NotImplementedError()
