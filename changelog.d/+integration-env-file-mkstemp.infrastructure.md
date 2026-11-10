Create the environment file used by the integration test helper with `tempfile.mkstemp`, so it is created atomically with owner-only permissions, and remove it when the test process exits.
