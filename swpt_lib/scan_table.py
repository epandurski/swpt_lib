from collections import deque
import sqlalchemy
from datetime import timedelta, datetime, timezone
from math import ceil
import time
import random

TD_ZERO = timedelta(seconds=0)
TD_MIN_SLEEPTIME = timedelta(milliseconds=10)

LAST_BLOCK_QUERY = """
SELECT pg_relation_size('{tablename}') / current_setting('block_size')::int
"""

TOTAL_ROWS_QUERY = """
SELECT reltuples::bigint
FROM pg_catalog.pg_class
WHERE relname = '{tablename}'
"""


class EndOfTableError(Exception):
    """The end of the table has been reached."""


class TableReader:
    def __init__(self, engine, table, blocks_per_query, columns=None):
        assert blocks_per_query >= 1
        self.engine = engine
        self.table = table
        self.table_query = sqlalchemy.select(columns or table.columns)
        self.blocks_per_query = blocks_per_query
        self.current_block = -1
        self.queue = deque()

    def _ensure_valid_current_block(self):
        last_block = self.engine.execute(LAST_BLOCK_QUERY.format(tablename=self.table.name))
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


class TableScanner:
    table = None  # model.__table__`
    columns = None
    blocks_per_query = 40
    target_beat_duration = timedelta(milliseconds=25)

    def __init__(self, engine, completion_goal: timedelta):
        assert completion_goal > TD_ZERO
        self.__engine = engine
        self.__reader = TableReader(self.__engine, self.table, self.blocks_per_query, self.columns)
        self.__completion_goal = completion_goal

    def __set_rhythm(self) -> None:
        completion_goal = self.__completion_goal
        target_number_of_beats = max(1, completion_goal // self.target_beat_duration)
        total_rows = self.__engine.execute(TOTAL_ROWS_QUERY.format(tablename=self.table.name)).scalar()
        self.__rows_per_beat = ceil(total_rows / target_number_of_beats + 0.1)
        number_of_beats = ceil(total_rows / self.__rows_per_beat) or 1
        self.__beat_duration = completion_goal / number_of_beats
        self.__saved_time = TD_ZERO
        current_ts = datetime.now(tz=timezone.utc)
        self.__last_beat_ended_at = current_ts
        self.__reset_rhythm_at = current_ts + completion_goal

    def __calc_elapsed_time(self) -> timedelta:
        current_ts = datetime.now(tz=timezone.utc)
        elapsed_time = current_ts - self.__last_beat_ended_at
        self.__last_beat_ended_at = current_ts
        return elapsed_time

    def __beat(self) -> None:
        rows = self.__reader.read_rows(count=self.__rows_per_beat)
        self.process_rows(rows)
        self.__saved_time += self.__beat_duration - self.__calc_elapsed_time()
        if self.__saved_time > TD_MIN_SLEEPTIME:
            time.sleep(self.__saved_time.total_seconds())
            self.__saved_time -= self.__calc_elapsed_time()

    def run(self):
        while True:
            self.__set_rhythm()
            while self.__last_beat_ended_at < self.__reset_rhythm_at:
                self.__beat()

    def process_rows(self, rows):
        raise NotImplementedError()
