# Release Process

- Bump the version number in github to an even number, and tag it.
  - version number is in: `b2/version.py`, `README.md`, and `setup.py`.
- Run full tests (currently: `pre-checkin.sh`)
- Commit and tag in git.  Version tags look like "v0.4.6"
- Upload to PyPI.
  - `cd ~/sandbox/B2_Command_Line_Tool`    # or wherever your git repository is
  - `rm -rf dist ; python setup.py sdist`
  - `twine upload dist/*`
- Bump the version number to an odd number and commit.

