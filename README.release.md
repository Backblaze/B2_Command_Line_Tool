# Release Process

- Bump the version number to an even number.
  - version number is in: `b2/version.py`, `README.md`, and `setup.py`.
- Update the release history in README.md.
- Copy the main usage string (from `python -m b2`) to README.md.
- Run full tests (currently: `pre-commit.sh`)
- Commit and push to GitHub, then wait for build to complete successfully.
- Tag in git and push tag to origin.  (Version tags look like "v0.4.6".)
- Upload to PyPI.
  - `cd ~/sandbox/B2_Command_Line_Tool`    # or wherever your git repository is
  - `rm -rf dist ; python setup.py sdist`
  - `twine upload dist/*`
- Bump the version number to an odd number and commit.
- Update https://www.backblaze.com/b2/docs/quick_command_line.html
