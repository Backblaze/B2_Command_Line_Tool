# Release Process

- Run `nox -s make_release_commit -- X.Y.Z` where `X.Y.Z` is the version you're releasing
- Copy the main usage string (from `b2 --help`) to `README.md`. Handy command for consistent format: `COLUMNS=4000 b2 --help | awk '/^usages:/ {p=1; next} p {sub(/^ */, "", $0); print}'`

