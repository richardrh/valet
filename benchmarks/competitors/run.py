"""Time Valet and Great Expectations on the same CSV and Parquet data.

Setup: uv venv --python 3.12 venv && uv pip install -r requirements.txt
Usage: venv/bin/python run.py WORKDIR ROWS VALET_BINARY [REPEATS]
WORKDIR should be disk-backed (not a RAM /tmp). Build Valet as CI does:
CGO_ENABLED=0 go build -trimpath -o valet ./cmd/valet

Each run is a fresh process measured by /usr/bin/time (wall seconds and peak
RSS). Every run's failure counts must match expected.json or the run is
reported as wrong. Prints a Markdown table of medians.
"""

import json
import statistics
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
VALET_CODES = {"ID_NOT_NULL": "id_not_null", "AMOUNT_RANGE": "amount_range", "QUANTITY_RANGE": "quantity_range", "STATUS_ALLOWED": "status_allowed"}


def valet(binary: str, workers: int):
    def command(fmt: str, data: Path) -> list[str]:
        return [binary, "validate", "-p", str(HERE / f"valet-{fmt}.yml"), "-i", str(data), "--workers", str(workers)]

    def counts(stdout: str) -> dict:
        summary = json.loads(stdout)
        return {"rows": summary["total"], **{VALET_CODES[k]: v for k, v in summary["rule_counts"].items()}}

    return command, counts


def python_tool(script: str):
    def command(fmt: str, data: Path) -> list[str]:
        return [sys.executable, str(HERE / script), str(data)]

    return command, lambda stdout: json.loads(stdout.strip().splitlines()[-1])


def timed(command: list[str]) -> tuple[float, int, str]:
    proc = subprocess.run(["/usr/bin/time", "-f", "%e %M", *command], capture_output=True, text=True)
    wall, rss_kib = proc.stderr.strip().splitlines()[-1].split()
    return float(wall), int(rss_kib), proc.stdout


def main() -> None:
    work, rows, binary = Path(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
    repeats = int(sys.argv[4]) if len(sys.argv) > 4 else 3
    data = work / f"rows-{rows}"
    if not (data / "expected.json").exists():
        subprocess.run([sys.executable, str(HERE / "generate.py"), str(data), str(rows)], check=True)
    expected = json.loads((data / "expected.json").read_text())
    tools = {
        "Valet (1 worker)": valet(binary, 1),
        "Valet (4 workers)": valet(binary, 4),
        "Great Expectations (pandas)": python_tool("ge_run.py"),
    }
    print(f"{rows:,} rows; repeats={repeats}; medians\n")
    print("| Format | Tool | Wall s | Peak RSS MiB | Counts correct |")
    print("|---|---|---:|---:|---|")
    for fmt in ("csv", "parquet"):
        path = data / f"bench.{fmt}"
        for name, (command, counts) in tools.items():
            walls, rss, correct = [], [], True
            for _ in range(repeats):
                wall, kib, stdout = timed(command(fmt, path))
                walls.append(wall)
                rss.append(kib / 1024)
                try:
                    got = counts(stdout)
                except (ValueError, KeyError, IndexError):
                    got = None
                if got != expected:
                    correct = False
                    print(f"<!-- {name} {fmt}: got {got}, want {expected} -->", file=sys.stderr)
            print(f"| {fmt} ({path.stat().st_size / 2**20:.0f} MiB) | {name} | {statistics.median(walls):.2f} "
                  f"| {statistics.median(rss):.0f} | {'yes' if correct else 'NO'} |", flush=True)


if __name__ == "__main__":
    main()
