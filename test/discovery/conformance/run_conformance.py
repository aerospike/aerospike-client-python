"""L1 conformance runner: run the shared discovery fixtures and write conformance-report.json.

translate, merge and dns cases run through the C client's conformance entry points in the installed
aerospike extension (see ../c_discovery.py); dns cases query the fixture set's fixture-server.py. The
client has no entry point for the other kinds, so their cases are recorded as unsupported, with the
reason declared for each in python-declared.yaml. The runner then asserts that every result equals
its declaration, in both directions, and exits non-zero otherwise.

Usage: run_conformance.py <fixtures directory> <declarations file> <report file>
"""

import json
import subprocess
import sys
import urllib.request
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import c_discovery  # noqa: E402

RUNTIME = "python"
FIXTURE_ROOTS = ("discovery", "tls")
FIXTURE_KINDS = {"translate", "collision", "merge", "dns", "aws-tags", "tls"}
FIXTURE_SERVER = Path("discovery/providers/fixture-server.py")
CLIENT_VERSION_FILE = Path(__file__).resolve().parents[3] / "VERSION"

# Error codes of the C client (as_status.h) for each fixture error category
ERROR_CODES = {"translation-miss": -25, "discovery-failed": -27}


class Mismatch(Exception):
    """The client's result differs from the fixture's expectation."""


def load_fixtures(directory):
    """Return every fixture case as (file, kind, row), in file and row order."""
    cases = []
    for path in sorted(p for root in FIXTURE_ROOTS for p in (directory / root).rglob("*.yaml")):
        data = yaml.safe_load(path.read_text())
        if not isinstance(data, dict) or data.get("kind") not in FIXTURE_KINDS:
            continue
        relative = path.relative_to(directory).as_posix()
        for row in data["rows"]:
            cases.append((relative, data["kind"], row))
    if not cases:
        raise SystemExit(f"No fixture cases under {directory}")
    return cases


def host_key(host):
    return (str(host["host"]), host["port"], host.get("tlsName"))


def check_error(actual, expected):
    category = expected["category"]
    if category not in ERROR_CODES:
        raise Mismatch(f"unknown error category {category}")
    if actual.code != ERROR_CODES[category]:
        raise Mismatch(f"expected {category} (code {ERROR_CODES[category]}), got {actual}")
    facts = expected.get("messageContains") or []
    if isinstance(facts, dict):
        facts = facts.values()
    missing = [str(fact) for fact in facts if str(fact) not in actual.message]
    if missing:
        raise Mismatch(f"message {actual.message!r} does not contain {missing}")


def check_hosts(name, actual, expected, ordered):
    actual_keys = [host_key(host) for host in actual]
    expected_keys = [host_key(host) for host in expected]
    if not ordered:
        actual_keys.sort(key=str)
        expected_keys.sort(key=str)
    if actual_keys != expected_keys:
        raise Mismatch(f"{name}: expected {expected_keys}, got {actual_keys}")


def run_translate(row, _):
    config = row["config"] or {}
    translation = config.get("translation") or {}
    kind = translation.get("provider", "static-map")
    entries = translation.get("static_map") or translation.get("port_map") or config.get("ip_map") or {}
    expected = row["expect"]
    try:
        actual = c_discovery.translate(kind, translation.get("on_miss", "pass_through"), entries, row["input"])
    except c_discovery.CError as err:
        if "error" not in expected:
            raise Mismatch(f"expected {expected}, got {err}") from None
        check_error(err, expected["error"])
        return
    if "error" in expected:
        raise Mismatch(f"expected {expected['error']['category']}, got {actual}")
    check_hosts("endpoint", [actual], [expected], ordered=True)


def run_merge(row, _):
    config = row["config"]
    policy = (config.get("discovery") or {}).get("merge_policy", "merge")
    snapshots = c_discovery.merge(policy, config["seeds"], row["input"]["discoveryResults"])
    expected = row["expect"]["snapshots"]
    if len(snapshots) != len(expected):
        raise Mismatch(f"expected {len(expected)} snapshots, got {len(snapshots)}")
    for i, (actual, expected_seeds) in enumerate(zip(snapshots, expected)):
        check_hosts(f"snapshot {i}", actual, expected_seeds, ordered=True)


def run_dns(row, fixture_server):
    fixture_server.start()
    dns = dict(row["config"]["discovery"]["dns"])
    dns["servers"] = [f"127.0.0.1:{fixture_server.dns_port}"]
    provider = c_discovery.DnsSeedProvider(dns)
    try:
        results = []
        for i, (refresh, expected) in enumerate(zip(row["input"]["refreshes"], row["expect"]["refreshes"])):
            fixture_server.select(refresh)
            try:
                hosts = provider.refresh(i)
            except c_discovery.CError as err:
                if "error" not in expected:
                    raise Mismatch(f"refresh {i}: expected hosts, got {err}") from None
                check_error(err, expected["error"])
                results.append(None)
                continue
            if "error" in expected:
                raise Mismatch(f"refresh {i}: expected {expected['error']['category']}, got {hosts}")
            check_hosts(f"refresh {i} hosts", hosts, expected["hosts"], ordered=False)
            results.append(hosts)
    finally:
        provider.close()

    # The provider replaces the seeds when the client has no static seeds
    snapshots = c_discovery.merge("discovery_only", [], results)
    for i, (seeds, expected) in enumerate(zip(snapshots, row["expect"]["refreshes"])):
        check_hosts(f"refresh {i} seeds", seeds, expected["seeds"], ordered=False)


RUNNERS = {"translate": run_translate, "merge": run_merge, "dns": run_dns}


class FixtureServer:
    """The fixture set's DNS and EC2 server, started on first use."""

    def __init__(self, fixtures_dir):
        self._script = fixtures_dir / FIXTURE_SERVER
        self._process = None
        self.dns_port = None
        self._http_port = None

    def start(self):
        if self._process:
            return
        self._process = subprocess.Popen(
            [sys.executable, str(self._script)], stdout=subprocess.PIPE, text=True
        )
        ports = json.loads(self._process.stdout.readline())
        self.dns_port = ports["dns"]
        self._http_port = ports["http"]

    def select(self, refresh):
        body = json.dumps({"refresh": refresh, "region": None}).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{self._http_port}/fixture/select",
            data=body,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()

    def stop(self):
        if self._process:
            self._process.terminate()
            self._process.wait()
            self._process = None


def run_case(kind, row, declared_reason, fixture_server):
    """Return the result and reason of one fixture case."""
    if row.get("state") == "blocked":
        return "blocked", row["blockedBy"]
    runner = RUNNERS.get(kind)
    if runner is None:
        return "unsupported", declared_reason
    try:
        runner(row, fixture_server)
    except (Mismatch, c_discovery.CError) as err:
        return "fail", str(err)
    return "pass", None


def load_declarations(path, cases):
    declared = {}
    known = {(file_name, row["case"]) for file_name, _, row in cases}
    for file_name, file_cases in (yaml.safe_load(path.read_text()) or {}).items():
        for case_name, body in file_cases.items():
            if (file_name, case_name) not in known:
                raise SystemExit(f"{path}: {file_name}:{case_name} is not a fixture case")
            declared[(file_name, case_name)] = body
    return declared


def main(fixtures_dir, declarations_file, report_file):
    cases = load_fixtures(fixtures_dir)
    declared = load_declarations(declarations_file, cases)
    fixture_server = FixtureServer(fixtures_dir)
    rows = []
    mismatches = []

    try:
        for file_name, kind, row in cases:
            case_name = row["case"]
            declaration = declared.get((file_name, case_name), {})
            result, reason = run_case(kind, row, declaration.get("reason", ""), fixture_server)
            report_row = {"file": file_name, "case": case_name, "result": result}
            if reason:
                report_row["reason"] = reason
            rows.append(report_row)
            default = "blocked" if row.get("state") == "blocked" else "pass"
            expected = declaration.get("result", default)
            if result != expected:
                detail = f": {reason}" if result == "fail" else ""
                mismatches.append(f"{file_name}:{case_name}: {result}, declared {expected}{detail}")
    finally:
        fixture_server.stop()

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
