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

TID_SCAN_QUERY = """
SELECT * FROM "{tablename}" WHERE ctid = ANY (ARRAY (
  SELECT ('(' || b.b || ',' || t.t || ')')::tid
  FROM generate_series({first_block:d}, {last_block:d}) AS b(b),
       generate_series(0, current_setting('block_size')::int / 32) AS t(t)
))
"""


class EndOfTableError(Exception):
    """The end of the table has been reached."""


class TableReader:
    def __init__(self, db, table, blocks_per_query):
        assert blocks_per_query >= 1
        self.db = db
        self.table = table  # model.__table__`
        self.blocks_per_query = blocks_per_query
        self.current_block = -1
        self.queue = []

    def _ensure_valid_current_block(self):
        last_block = self.db.engine.execute(LAST_BLOCK_QUERY.format(tablename=self.table.name))
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
        tid_scan = self.db.engine.execute(TID_SCAN_QUERY.format(
            tablename=self.table.name,
            first_block=first_block,
            last_block=self.current_block - 1,
        ))
        return tid_scan.fetchall()

    def get_rows(self, count) -> list:
        """Return a list of at most `count` rows."""

        rows = self.queue
        while len(rows) < count:
            try:
                rows.extend(self._advance_current_block())
            except EndOfTableError:
                self.current_block = 0
                break
        self.queue = rows[count:]  # TODO: Eliminate the unnecessary copy?
        return rows[:count]


class TableScanner:
    db = None
    table = None
    blocks_per_query = 1
    target_beat_duration = timedelta(milliseconds=10)

    def __init__(self, completion_goal: timedelta):
        assert completion_goal > TD_ZERO
        self.reader = TableReader(self.db, self.table, self.blocks_per_query)
        self.completion_goal = completion_goal

    def _set_rhythm(self) -> None:
        target_number_of_beats = max(1, self.completion_goal // self.target_beat_duration)
        total_rows = self.db.engine.execute(TOTAL_ROWS_QUERY.format(tablename=self.table.name)).scalar()
        self.rows_per_beat = ceil(total_rows / target_number_of_beats + 0.1)
        number_of_beats = ceil(total_rows / self.rows_per_beat) or 1
        self.beat_duration = self.completion_goal / number_of_beats
        self.saved_time = TD_ZERO
        current_ts = datetime.now(tz=timezone.utc)
        self.last_beat_ended_at = current_ts
        self.reset_rhythm_at = current_ts + self.completion_goal

    def _calc_saved_time(self, time_bonus: timedelta) -> timedelta:
        current_ts = datetime.now(tz=timezone.utc)
        elapsed_time = current_ts - self.last_beat_ended_at
        self.last_beat_ended_at = current_ts
        return time_bonus - elapsed_time

    def _beat(self) -> None:
        rows = self.reader.get_rows(count=self.rows_per_beat)
        self.process_rows(rows)
        self.saved_time += self._calc_saved_time(time_bonus=self.beat_duration)
        if self.saved_time > TD_MIN_SLEEPTIME:
            time.sleep(self.saved_time.total_seconds())
            self.saved_time += self._calc_saved_time(time_bonus=TD_ZERO)

    def run(self):
        while True:
            self._set_rhythm()
            while self.last_beat_ended_at < self.reset_rhythm_at:
                self._beat()

    def process_rows(self, rows):
        raise NotImplementedError()
