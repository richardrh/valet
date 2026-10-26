"""Generate the competitor benchmark dataset as CSV and Parquet.

Columns: id int64 (nullable), amount decimal(18,2), quantity int64, status
string. Violations are placed deterministically so every tool's failure
counts can be checked against expected.json.
"""

import json
import sys
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

ID_NULL, AMOUNT_BAD, QUANTITY_BAD, STATUS_BAD = 101, 103, 107, 109


def build(rows: int) -> pa.Table:
    i = np.arange(rows, dtype=np.int64)
    ids = pa.array(i, mask=(i % ID_NULL == 0))
    # Cents in [0, 1_000_000]; 10000.00 itself is a valid inclusive bound.
    cents = (i * 37) % 1_000_001
    cents[i % AMOUNT_BAD == 0] = 1_000_001  # 10000.01, just above max
    words = np.zeros((rows, 2), dtype=np.int64)
    words[:, 0] = cents
    amount = pa.Array.from_buffers(pa.decimal128(18, 2), rows, [None, pa.py_buffer(words)])
    quantity = (i % 1000) + 1
    quantity[i % QUANTITY_BAD == 0] = 0
    status = np.array(["OPEN", "PAID", "CANCELLED"], dtype=object)[i % 3]
    status[i % STATUS_BAD == 0] = "UNKNOWN"
    return pa.table({"id": ids, "amount": amount, "quantity": quantity, "status": pa.array(status, pa.string())})


def expected(rows: int) -> dict:
    def hits(mod: int) -> int:
        return (rows - 1) // mod + 1 if rows else 0

    return {
        "rows": rows,
        "id_not_null": hits(ID_NULL),
        "amount_range": hits(AMOUNT_BAD),
        "quantity_range": hits(QUANTITY_BAD),
        "status_allowed": hits(STATUS_BAD),
    }


def main() -> None:
    out, rows = Path(sys.argv[1]), int(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)
    table = build(rows)
    pacsv.write_csv(table, out / "bench.csv", pacsv.WriteOptions(quoting_style="none"))
    pq.write_table(table, out / "bench.parquet", compression="snappy", row_group_size=1 << 20)
    (out / "expected.json").write_text(json.dumps(expected(rows), indent=2) + "\n")


if __name__ == "__main__":
    main()
