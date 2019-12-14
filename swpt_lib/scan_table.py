from datetime import timedelta
from math import ceil
import random

TD_ZERO = timedelta(seconds=0)

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

    def get_rows(self, count=1) -> list:
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
    target_turn_duration = timedelta(milliseconds=10)

    def __init__(self, completion_goal: timedelta):
        assert completion_goal > TD_ZERO
        self.table_reader = TableReader(self.db, self.table, self.blocks_per_query)
        self.completion_goal = completion_goal
        self.remaining_rows = 0
        self.rows_per_turn = 1

    def set_goal(self):
        target_number_of_turns = max(1, self.completion_goal // self.target_turn_duration)
        total_rows = max(0, self.db.engine.execute(TOTAL_ROWS_QUERY.format(tablename=self.table.name)).scalar())
        self.rows_per_turn = ceil(total_rows / target_number_of_turns + 0.1)
        number_of_turns = ceil(total_rows / self.rows_per_turn) or 1
        self.turn_duration = self.completion_goal / number_of_turns
        self.accumulated_delay = TD_ZERO

    def execute_turn(self):
        pass

    def run(self):
        while True:
            rows = self.table_reader.get_rows()
        

#  explain select * from test where ctid = any (array['(10,2)'::tid, '(10,3)'::tid]);
