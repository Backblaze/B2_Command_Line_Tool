# sdkharness contract

This directory exposes version-matched quality checks to the centralized
[`sdkharness`](https://github.com/backblaze-labs/demand-side-ai/tree/main/sdkharness).
The repository owns the executable assertions; sdkharness owns the canonical
scenario, simulator, invocation, evidence, fleet report, and notification.

`tests.tsv` is the machine-readable entry point. Schema 1 has four
tab-separated columns: `test_level`, `scenario`, `target`, and `executable`.
Executables print one five-field, tab-separated result:

```text
SDKHARNESS_RESULT	health	golden-path	PASS	-
```

The customer-health executable accepts only a literal loopback HTTP simulator
URL. It invokes the latest stable CLI module from this checkout and never uses
a production B2 endpoint or real credentials.

## Run one check locally

Nothing here touches B2. You need Python 3.10+ and Node 22+ (for the simulator).

```bash
# 1. This checkout in a virtualenv (the checks run the CLI and b2sdk from here)
python -m venv .venv && . .venv/bin/activate && pip install -e .

# 2. A local simulator (any one of these; it needs access to backblaze-labs/b2-simulator)
git clone https://github.com/backblaze-labs/b2-simulator /tmp/b2-simulator
node /tmp/b2-simulator/bin/simulator/serve.mjs --control > /tmp/sim.out &    # prints the URLs
# ...or use the simulator embedded in the harness: sdkharness/bin/simulator/serve.mjs

# 3. Read the URLs it printed
export SDKHARNESS_SIMULATOR_URL=$(sed -n 's/^SIMULATOR-LISTENING \(http:.*\)/\1/p' /tmp/sim.out)
export SDKHARNESS_SIMULATOR_CONTROL_URL=$(sed -n 's/^SIMULATOR-CONTROL \(.*\)/\1/p' /tmp/sim.out)

# 4. A bucket for the conformance and health checks (resilience makes its own)
python - <<'PY'
import os
from b2sdk.v3 import B2Api, InMemoryAccountInfo
api = B2Api(InMemoryAccountInfo())
api.authorize_account('test-key-id', 'test-key', realm=os.environ['SDKHARNESS_SIMULATOR_URL'])
api.create_bucket('sdkharness-conformance', 'allPrivate')
PY
```

The standalone simulator also exports the same values as `B2SIM_URL`,
`B2SIM_HTTPS_URL`, `B2SIM_CONTROL_URL` and `B2SIM_CA` (its `bin/lib/simulator.sh`
helper); the checks read the `SDKHARNESS_SIMULATOR_*` names above.

Each scenario is run by the executable in `tests.tsv` (column 4):

```bash
# conformance (executable: conformance-file-lifecycle.py or conformance-files-upload.py)
SDKHARNESS_TEST_LEVEL=conformance SDKHARNESS_SCENARIO=files.hide \
  B2_TEST_APPLICATION_KEY_ID=test-key-id B2_TEST_APPLICATION_KEY=test-key \
  B2_BUCKET_NAME=sdkharness-conformance .sdkharness/tests/conformance-file-lifecycle.py

# resilience (one injected fault; needs the control URL, i.e. serve.mjs --control)
SDKHARNESS_TEST_LEVEL=resilience SDKHARNESS_SCENARIO=api.backoff_503 \
  .sdkharness/tests/resilience-upload.py

# customer health
HEALTHCHECK_REALM_URL=$SDKHARNESS_SIMULATOR_URL B2_TEST_APPLICATION_KEY_ID=test-key-id \
  B2_TEST_APPLICATION_KEY=test-key B2_BUCKET_NAME=sdkharness-conformance \
  .sdkharness/tests/health-golden-path.py
```

Each prints one `SDKHARNESS_RESULT` line and refuses any simulator URL that is not
`http://127.0.0.1:<port>`. Use a fresh simulator per resilience scenario: an
unconsumed injected fault from a failing scenario (for example `upload.retry_408`)
carries over to the next one. `api.retry_after_503` and `upload.retry_408` are known
SDK findings, not setup problems.
