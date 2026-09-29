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
