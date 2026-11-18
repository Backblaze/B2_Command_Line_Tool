# Releasing the B2 command-line tool

Releases use a reviewed commit on `master` and a maintainer-signed `vX.Y.Z` tag. The tag starts
[`.github/workflows/cd.yml`](.github/workflows/cd.yml), which builds the Python distribution,
publishes it to PyPI, uploads the Linux and DigiCert-signed Windows bundles to GitHub Releases,
and publishes the Docker Hub image for stable versions. The package version comes from the tag
through hatch-vcs; do not edit a version number in `pyproject.toml`.

## One-time publication setup

1. Configure the `b2` project on PyPI to trust GitHub Actions from owner `Backblaze`, repository
   `B2_Command_Line_Tool`, workflow `cd.yml`, and environment `pypi`. Create or protect the
   `pypi` GitHub environment before tagging. The PyPI job has `id-token: write` and no PyPI API
   token; the pinned PyPA publishing action uploads attestations with each distribution. Confirm
   this Trusted Publisher configuration before removing the old `B2_PYPI_PASSWORD` repository secret.
2. The trusted SSH public keys for release tags are in
   [`release-keys/maintainers.allowed_signers`](release-keys/maintainers.allowed_signers). A release
   manager must sign with the corresponding private key. Review additions or removals of trusted
   keys as release-security changes. The verifier reads the fetched `origin/master` key policy,
   so a key removed there is no longer accepted when checking an older release commit.
3. The release gate reads GitHub's Dependabot, code-scanning, secret-scanning, and repository
   advisory APIs. Configure `RELEASE_SECURITY_TOKEN` with read-only access to these sources,
   including **Repository security advisories: read**, to verify unpublished advisories in CI.
   Without that dedicated token, CI reports advisories as **UNVERIFIED**, because `GITHUB_TOKEN`
   can return only published advisories even when the API request succeeds. The other sources
   use `GITHUB_TOKEN` when the dedicated token is absent. Do not grant broader write permissions.
4. `SECURITY_GATE_STRICT` defaults to `false`. An unreadable source is printed as **UNVERIFIED**
   with a workflow warning and needs a manual Security-tab review; it is never treated as a clean
   scan. The code-scanning and secret-scanning alert APIs currently return 404 for this repository.
   Enable those features and verify the gate can read them before setting the repository variable
   `SECURITY_GATE_STRICT=true`; strict mode holds a release on any unreadable source.

## Prepare the release

1. Choose `X.Y.Z`, create its GitHub milestone, and make sure intended issues and PRs are resolved.
   Confirm `master` CI is green. Do not release with failing CI.
2. Run the local health check from a clean checkout, with the required `uv` version, `nox`, Go,
   GitHub CLI (`gh`), and `jq` installed. Authenticate `gh` as a maintainer with access to the
   repository's security sources, or provide a token with equivalent read access:

   ```bash
   gh auth login
   gh auth status
   nox -vs changie_check release_check lint unit build
   GITHUB_REPOSITORY=Backblaze/B2_Command_Line_Tool scripts/release-security-gate.sh
   ```

   The gate holds on readable open medium, high, or critical findings; it also holds on any open
   secret-scanning alert. A code owner must inspect each **UNVERIFIED** source on the Security tab
   before approving the release; a successful gate exit with **UNVERIFIED** sources is not a
   complete clearance. For a draft advisory fixed by this release, set
   `RELEASE_FIXES_ADVISORIES` to its GHSA ID for the gate, have a code owner confirm the fix, and
   publish that advisory after the release. Never put an unpublished advisory description in a
   public PR.
3. Update the main usage string in `README.md` from `b2 --help` and commit that update before
   running `make_release_commit`, which requires a clean checkout. A consistent extraction command is:

   ```bash
   COLUMNS=4000 b2 --help | awk '/^usages:/ {p=1; next} p {sub(/^ */, "", $0); print}'
   ```

4. Prepare Changie notes. Each change should have a fragment in `.changes/unreleased`. Preview
   the batch without modifying the checkout:

   ```bash
   go run github.com/miniscruff/changie@v1.24.0 batch vX.Y.Z --dry-run
   ```

5. On a release branch from `master`, run `nox -s make_release_commit -- X.Y.Z`. It checks that
   the CLI depends on a **released b2sdk** version, batches the fragments into
   `.changes/vX.Y.Z.md`, and rebuilds `CHANGELOG.md`. Review the notes and commit them. Open a
   release PR; a maintainer merges it after CI passes. The release command does not publish or tag.

## Tag and publish

1. Fetch the merged `master` commit. Set up Git SSH signing with the private key matching an
   in-repo trusted public key, then create and locally verify an annotated signed tag on the
   release commit:

   ```bash
   git config gpg.format ssh
   git config user.signingkey /path/to/maintainer-signing-key
   git tag -s vX.Y.Z -m vX.Y.Z <merged-release-commit>
   scripts/verify-release-tag.sh vX.Y.Z
   ```

   The verifier checks the signature against the current `origin/master` key file and confirms the tagged commit
   is on `origin/master`. Do not move a published tag; release a new patch version instead.
2. Push the tag only after the release PR and `master` CI are green. The tag-triggered workflow
   runs signature and security checks **before** any publishing job. A failed gate holds the
   release. Pre-release tags skip PyPI and Docker Hub publishing as in the existing workflow.
3. Check the GitHub release assets, the PyPI project page and its attestations, the Linux and
   Windows bundle jobs, and the Docker Hub tags/description. The Windows bundle still uses
   DigiCert signing; Linux signing behavior is unchanged. Publish any advisory fixed by this
   release after the tag is available.

## Local release-path dry run

Use a throwaway worktree to run `nox -s make_release_commit -- X.Y.Z`; inspect the generated
`.changes/vX.Y.Z.md` and `CHANGELOG.md`, then remove the worktree. This exercises release-note
assembly and the released-b2sdk check without creating a tag, uploading artifacts, or publishing
to PyPI or Docker Hub. The signed-tag, alert-gate, and release-note extraction scripts have local
tests in `test/unit/test_release_workflow.py`; GitHub environment permissions and the PyPI
Trusted Publisher must still be validated in a real maintainer-controlled release.

Release work may use stacked pull requests. Pull-request CI runs for every base branch so both
the Changie migration and its dependent release-workflow change are checked before merging.
