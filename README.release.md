# Release Process

- Update the release history in `CHANGELOG.md`:
  - Change "Unreleased" to the current release version and date.
  - Create empty "Unreleased" section.
  - Add proper link to the new release (at the bottom of the file). Use GitHub [compare feature](https://docs.github.com/en/free-pro-team@latest/github/committing-changes-to-your-project/comparing-commits#comparing-tags) between two tags.
  - Update "Unreleased" link (at the bottom of the file).
- Copy the main usage string (from `b2 --help`) to `README.md`. Handy command for consistent format: `COLUMNS=4000 b2 --help | awk '/^usages:/ {p=1; next} p {sub(/^ */, "", $0); print}'`
- Commit and push to a GitHub branch, then wait for CI workflow to complete successfully.
- Merge the PR
- Pull from GitHub
- Tag in git and push tag to `origin`.  (Version tags look like `v0.4.6`.)
  - `git tag vx.x.x`
  - `git push origin vx.x.x`
- Wait for CD workflow to complete successfully.
  - Verify that the GitHub release is created
  - Verify that the release has been uploaded to the PyPI
- Install using `pip` and verify that it gets the correct version:
  - `pip install -U b2`
- Update https://www.backblaze.com/b2/docs/quick_command_line.html if needed
