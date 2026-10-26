"""Validate the benchmark file with Great Expectations' pandas engine.

Prints the unexpected count per rule as JSON. Files load the way a GE user
would by default: pandas read_csv (amount becomes float64) or read_parquet
(amount becomes Python Decimal objects).
"""

import json
import sys

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd

RULES = {
    "id_not_null": gxe.ExpectColumnValuesToNotBeNull(column="id"),
    "amount_range": gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=0, max_value=10000),
    "quantity_range": gxe.ExpectColumnValuesToBeBetween(column="quantity", min_value=1, max_value=1000),
    "status_allowed": gxe.ExpectColumnValuesToBeInSet(column="status", value_set=["OPEN", "PAID", "CANCELLED"]),
}


def main() -> None:
    path = sys.argv[1]
    if path.endswith(".csv"):
        frame = pd.read_csv(path, dtype={"id": "Int64"})
    else:
        frame = pd.read_parquet(path)
    context = gx.get_context(mode="ephemeral")
    batch = (
        context.data_sources.add_pandas("bench")
        .add_dataframe_asset("bench")
        .add_batch_definition_whole_dataframe("all")
        .get_batch(batch_parameters={"dataframe": frame})
    )
    suite = context.suites.add(gx.ExpectationSuite(name="bench"))
    codes = {}
    for code, expectation in RULES.items():
        suite.add_expectation(expectation)
        codes[expectation.expectation_type, expectation.column] = code
    result = batch.validate(suite)
    counts = {}
    for r in result.results:
        cfg = r.expectation_config
        counts[codes[cfg.type, cfg.kwargs["column"]]] = r.result["unexpected_count"]
    print(json.dumps({"rows": len(frame), **counts}))


if __name__ == "__main__":
    main()
