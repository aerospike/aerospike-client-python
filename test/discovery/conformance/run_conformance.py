"""L1 conformance runner: run the shared discovery fixtures and write conformance-report.json.

The client has no entry point for any fixture kind yet, so every case that the fixture does not
block is recorded as unsupported with the reason below. The runner then asserts that every result
equals its declaration in python-declared.yaml, in both directions, and exits non-zero otherwise.

Usage: run_conformance.py <fixtures directory> <declarations file> <report file>
"""

import json
import sys
from pathlib import Path

import yaml

RUNTIME = "python"
FIXTURE_ROOTS = ("discovery", "tls")
CLIENT_VERSION_FILE = Path(__file__).resolve().parents[3] / "VERSION"

# Keep in step with the reasons in python-declared.yaml
UNSUPPORTED = {
    "translate": "no address translation entry point: ip_map is applied inside the C client's cluster tend",
    "collision": "no address translation entry point to detect a collision with",
    "merge": "no seed provider merge in the C client",
    "dns": "no DNS seed provider in the C client",
    "aws-tags": "no AWS tags seed provider in the C client",
    "tls": "no TLS name matching entry point: the C client matches names only inside a TLS handshake",
}


def load_fixtures(directory):
    """Return every fixture case as (file, case, kind, row), in file and row order."""
    cases = []
    for path in sorted(p for root in FIXTURE_ROOTS for p in (directory / root).rglob("*.yaml")):
        data = yaml.safe_load(path.read_text())
        if not isinstance(data, dict) or data.get("kind") not in UNSUPPORTED:
            continue
        relative = path.relative_to(directory).as_posix()
        for row in data["rows"]:
            cases.append((relative, row["case"], data["kind"], row))
    if not cases:
        raise SystemExit(f"No fixture cases under {directory}")
    return cases


def run_case(kind, row):
    """Return the result and reason of one fixture case."""
    if row.get("state") == "blocked":
        return "blocked", row["blockedBy"]
    return "unsupported", UNSUPPORTED[kind]


def load_declarations(path, cases):
    declared = {}
    known = {(file_name, case_name) for file_name, case_name, _, _ in cases}
    for file_name, file_cases in (yaml.safe_load(path.read_text()) or {}).items():
        for case_name, body in file_cases.items():
            if (file_name, case_name) not in known:
                raise SystemExit(f"{path}: {file_name}:{case_name} is not a fixture case")
            declared[(file_name, case_name)] = body["result"]
    return declared


def main(fixtures_dir, declarations_file, report_file):
    cases = load_fixtures(fixtures_dir)
    declared = load_declarations(declarations_file, cases)
    rows = []
    mismatches = []

    for file_name, case_name, kind, row in cases:
        result, reason = run_case(kind, row)
        rows.append({"file": file_name, "case": case_name, "result": result, "reason": reason})
        default = "blocked" if row.get("state") == "blocked" else "pass"
        expected = declared.get((file_name, case_name), default)
        if result != expected:
            mismatches.append(f"{file_name}:{case_name}: {result}, declared {expected}")

    counts = {result: 0 for result in ("pass", "fail", "unsupported", "blocked")}
    for row in rows:
        counts[row["result"]] += 1

    report = {
        "runtime": RUNTIME,
        "clientVersion": CLIENT_VERSION_FILE.read_text().strip(),
        "fixtureSetVersion": (fixtures_dir / "VERSION").read_text().strip(),
        "counts": counts,
        "rows": rows,
    }
    report_file.parent.mkdir(parents=True, exist_ok=True)
    report_file.write_text(json.dumps(report, indent=2) + "\n")
    print(f"{report_file}: {counts}")

    if mismatches:
        print("Results that differ from their declarations:", *mismatches, sep="\n  ", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit(__doc__)
    fixtures_dir = Path(sys.argv[1])
    declarations_file = Path(sys.argv[2])
    report_file = Path(sys.argv[3])
    sys.exit(main(fixtures_dir, declarations_file, report_file))
