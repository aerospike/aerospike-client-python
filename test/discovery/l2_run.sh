#!/usr/bin/env bash
#
# The Python client's own L2 tests, for the l2-proxy harness in aerospike-client-conformance
# (l2-proxy/README.md there), run by test/discovery/l5-aws.manifest as L2_RUN once per profile in a
# container on the client network with the topology's values file as its environment. Runs the
# profile's tests with the venv L5_BUILD wrote, so it needs no network.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
profile="${L2_PROFILE:?L2_PROFILE is required}"

# The harness's own row checks how the server starts; no Python test runs a client there.
if [ "$profile" = unresolved-access ]; then
	echo "L2-RUN profile=$profile no Python tests"
	exit 0
fi

DISCOVERY_FAIL_ON_SKIP=1 PYTHONDONTWRITEBYTECODE=1 \
	.l5-venv/bin/python -m pytest test/discovery/test_l2_*.py -p no:cacheprovider -rA
