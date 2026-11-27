#!/usr/bin/env bash
# Start the B2 Simulator from a checkout, then run a command against its HTTPS listener.
#
# Usage: scripts/with-b2-simulator.sh SIMULATOR_DIR COMMAND [ARG...]
#
# The command runs with B2_ENVIRONMENT and B2_TEST_ENVIRONMENT set to the Simulator's loopback
# URL, and with REQUESTS_CA_BUNDLE and SSL_CERT_FILE set to certifi's public roots plus the
# Simulator's loopback certificate. B2_SIMULATOR_CA_BUNDLE chooses where that bundle is written
# (default: $RUNNER_TEMP/b2-simulator-ca.pem); a container must be able to read it. The command's
# exit status is returned, and the Simulator is stopped when the script exits.
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: $0 SIMULATOR_DIR COMMAND [ARG...]" >&2
  exit 2
fi
simulator_dir="$1"
shift

temp_dir="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
simulator_log="$temp_dir/b2-simulator.log"
node "$simulator_dir/bin/simulator/serve.mjs" --custom-upload-timestamps >"$simulator_log" 2>&1 &
simulator_pid=$!
trap 'kill "$simulator_pid" 2>/dev/null || true; wait "$simulator_pid" 2>/dev/null || true' EXIT

B2_ENVIRONMENT=""
for _ in $(seq 1 150); do
  B2_ENVIRONMENT="$(tr -d '\r' <"$simulator_log" | sed -n 's|^SIMULATOR-LISTENING \(https://127\.0\.0\.1:[0-9][0-9]*\)$|\1|p' | head -1)"
  if [ -n "$B2_ENVIRONMENT" ]; then break; fi
  if ! kill -0 "$simulator_pid" 2>/dev/null; then
    cat "$simulator_log"
    exit 1
  fi
  sleep 0.2
done
if [ -z "$B2_ENVIRONMENT" ]; then
  cat "$simulator_log"
  exit 1
fi
export B2_ENVIRONMENT
B2_TEST_ENVIRONMENT="$B2_ENVIRONMENT"
export B2_TEST_ENVIRONMENT

certifi_bundle="$(uv run python -c 'import certifi; print(certifi.where())')"
simulator_ca_bundle="${B2_SIMULATOR_CA_BUNDLE:-$temp_dir/b2-simulator-ca.pem}"
cp "$certifi_bundle" "$simulator_ca_bundle"
cat "$simulator_dir/bin/simulator/loopback-cert.pem" >>"$simulator_ca_bundle"
REQUESTS_CA_BUNDLE="$simulator_ca_bundle"
export REQUESTS_CA_BUNDLE
SSL_CERT_FILE="$REQUESTS_CA_BUNDLE"
export SSL_CERT_FILE

"$@"
