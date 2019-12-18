from typing import List, Tuple, Optional
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
"""The default number of blocks to be retrieved per query."""

DEFAULT_TARGET_BEAT_DURATION = 25
"""The default target duration of scanning beats in milliseconds."""


class TableReader:
    """Reads a table sequentially, ad infinitum."""

    class EndOfTableError(Exception):
        """The end of the table has been reached."""

    LAST_BLOCK_QUERY = """SELECT pg_relation_size('{tablename}') / current_setting('block_size')::int"""

    def __init__(self, engine: Connectable, table: Table, blocks_per_query: int, columns: List[ColumnElement] = None):
        assert blocks_per_query >= 1
        self.engine = engine
        self.table = table
        self.table_query = sqlalchemy.select(columns or table.columns)
        self.blocks_per_query = blocks_per_query
        self.current_block = -1
        self.queue: deque = deque()

    def _ensure_valid_current_block(self):
        last_block = self.engine.execute(self.LAST_BLOCK_QUERY.format(tablename=self.table.name))
        total_blocks = last_block.scalar() + 1
        assert total_blocks > 0
        if self.current_block < 0:
            self.current_block = random.randrange(total_blocks)
        if self.current_block >= total_blocks:
            raise self.EndOfTableError()

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

        rows = []
        while len(self.queue) < count:
            try:
                self.queue.extend(self._advance_current_block())
            except self.EndOfTableError:
                self.current_block = 0
                break
        for _ in range(count):
            try:
                rows.append(self.queue.popleft())
            except IndexError:
                break
        return rows


class Rhythm:
    """A helper class to maintain a constant scanning rhythm."""

    beat_duration: timedelta
    last_beat_ended_at: datetime
    rhythm_ends_at: datetime
    extra_time: timedelta

    TD_ZERO = timedelta(seconds=0)
    TD_MIN_SLEEPTIME = timedelta(milliseconds=10)

    def __init__(self, completion_goal: timedelta, number_of_beats: int):
        assert completion_goal > self.TD_ZERO
        assert number_of_beats >= 1
        current_ts = datetime.now(tz=timezone.utc)
        self.beat_duration = completion_goal / number_of_beats
        self.last_beat_ended_at = current_ts
        self.rhythm_ends_at = current_ts + completion_goal
        self.extra_time = self.TD_ZERO

    def _register_elapsed_time(self) -> timedelta:
        current_ts = datetime.now(tz=timezone.utc)
        elapsed_time = current_ts - self.last_beat_ended_at
        self.last_beat_ended_at = current_ts
        return elapsed_time

    def register_beat(self):
        self.extra_time += self.beat_duration - self._register_elapsed_time()
        if self.extra_time > self.TD_MIN_SLEEPTIME:
            time.sleep(self.extra_time.total_seconds())
            self.extra_time -= self._register_elapsed_time()

    @property
    def has_ended(self):
        return self.last_beat_ended_at >= self.rhythm_ends_at


class TableScanner:
    """A table-scanner super-class. Each subclass can define the following
    attributes:

      `table`
         The :class:`sqlalchemy.schema.Table` that will be scanned
         (`model.__table__` if declarative base is used).

         **Must be defined in the subclass.**

      `columns`
         An optional list of
         :class:`sqlalchemy.sql.expression.ColumnElement` instances
         to be be retrieved for each row. Most of the time it will be
         a list of :class:`~sqlalchemy.schema.Column`
         instances. Defaults to `table.columns`.

      `blocks_per_query`
         The number of database pages (blocks) to be retrieved per
         query. Defaults to ``DEFAULT_BLOCKS_PER_QUERY``. It might be
         a good idea to increase this number when the size of the
         table row is big.

      `target_beat_duration`
         The scanning of the table is done in a sequence of
         "beats". This attribute determines the ideal duration in
         milliseconds of those beats. The value should be big enough
         so that, on average, all the operations performed on table's
         rows could be completed within this interval. Setting this
         value too high may have the effect of too many rows being
         processed simultaneously in one beat. The default value is
         ``DEFAULT_TARGET_BEAT_DURATION``.

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

    TOTAL_ROWS_QUERY = """SELECT reltuples::bigint FROM pg_catalog.pg_class WHERE relname = '{tablename}'"""

    table: Optional[Table] = None
    columns: Optional[List[ColumnElement]] = None
    blocks_per_query: int = DEFAULT_BLOCKS_PER_QUERY
    target_beat_duration: int = DEFAULT_TARGET_BEAT_DURATION

    def __calc_rhythm(self, total_rows: int, completion_goal: timedelta) -> Tuple[Rhythm, int]:
        assert total_rows >= 0
        assert self.target_beat_duration > 0
        target_number_of_beats = max(1, completion_goal // timedelta(milliseconds=self.target_beat_duration))
        rows_per_beat = max(1, ceil(total_rows / target_number_of_beats))
        number_of_beats = max(1, ceil(total_rows / rows_per_beat))
        return Rhythm(completion_goal, number_of_beats), rows_per_beat

    def run(self, engine: Connectable, completion_goal: timedelta):
        """Scan table continuously.

        The table is scanned sequentially, starting from a random
        row. During the scan :meth:`process_rows` will be continuously
        invoked with a list of rows. When the end of the table is
        reached, the scan continues from the beginning, ad infinitum.

        :param engine: SQLAlchemy engine

        :param completion_goal: The time interval in which the whole table
          should be processed. This is merely an approximate goal. In
          reality, scans can take any amount of time.

        """

        assert self.table is not None, '"table" must be defined in the subclass.'
        reader = TableReader(engine, self.table, self.blocks_per_query, self.columns)
        while True:
            total_rows = engine.execute(self.TOTAL_ROWS_QUERY.format(tablename=self.table.name)).scalar()
            rhythm, rows_per_beat = self.__calc_rhythm(total_rows, completion_goal)
            while not rhythm.has_ended:
                rows = reader.read_rows(count=rows_per_beat)
                self.process_rows(rows)
                rhythm.register_beat()

    def process_rows(self, rows: list) -> None:  # pragma: no cover
        """Process a list or rows.

        **Must be defined in the subclass.**

        :param rows: A list of table rows. It could be an empty list.

        """

        raise NotImplementedError()
