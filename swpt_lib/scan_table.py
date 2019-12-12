from datetime import timedelta
import random

TOTAL_BLOCKS_QUERY = """
SELECT pg_relation_size('{tablename}') / current_setting('block_size')::int
"""

TID_SCAN_QUERY = """
SELECT * FROM {tablename} WHERE ctid = ANY (ARRAY (
  SELECT ('(' || b.b || ',' || t.t || ')')::tid
  FROM generate_series({first_block:d}, {last_block:d}) AS b(b),
       generate_series(0, current_setting('block_size')::int / 32) AS t(t)
))
"""


class TableScanner:
    table = None  # Must be defined in the subclass (`model.__table__`).
    blocks_per_turn = 1

    def __init__(self, db, completion_goal: timedelta):
        self.db = db
        self.completion_goal = completion_goal
        self.current_block = -1
        self.total_blocks = 1

    def __update_current_block(self) -> int:
        result = self.db.engine.execute(TOTAL_BLOCKS_QUERY.format(tablename=self.table.name))
        self.total_blocks = result.scalar() + 1
        assert self.total_blocks > 0
        if self.current_block < 0:
            self.current_block = random.randrange(self.total_blocks)
        if self.current_block >= self.total_blocks:
            self.current_block = 0
        return self.current_block

    def execute_turn(self):
        """Process some rows."""

        start_block = self.__update_current_block()
        self.current_block += self.blocks_per_turn
        rows = self.db.engine.execute(TID_SCAN_QUERY.format(
            tablename=self.table.name,
            first_block=start_block,
            last_block=self.current_block - 1,
        ))
        for row in rows:
            self.process_row(row)

    def run(self):
        """Scan the table continuously."""

    def process_row(self, row):
        return NotImplementedError()


#  explain select * from test where ctid = any (array['(10,2)'::tid, '(10,3)'::tid]);
