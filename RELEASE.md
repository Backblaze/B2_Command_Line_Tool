# Releasing the B2 CLI

A release is a changelog commit on `master` (titled `release X.Y.Z` or `vX.Y.Z`, for example `9ea898a`,
v5.0.2) and a `vX.Y.Z` tag on it. The version comes from the git tag (hatch-vcs); no file holds it. Pushing the tag runs
[`cd.yml`](.github/workflows/cd.yml), which publishes everything.

## Before you start

- `master` is green in CI, including the live-B2 integration jobs.
- `pyproject.toml` depends on a released `b2sdk` (no git URL); `make_release_commit` refuses otherwise.
  If the release needs a new SDK, release `b2-sdk-python` first and bump the pin in its own PR.
- `changelog.d/` holds a fragment for every change since the last tag, and `nox -s towncrier_check`
  passes. Review the wording: it becomes the GitHub release body. `towncrier build --draft --version X.Y.Z`
  previews the section.

## Versioning

Semantic versioning. A `removed` or breaking `changed` fragment needs a major bump and, usually, a new
ApiVer (`b2vN`) so scripts on the old interface keep working. `added`, `changed` or `deprecated` means
minor; only `fixed`, `doc` and `infrastructure` means patch. Maintainers usually batch trivial changes into the next
release rather than cutting one for each.

## Steps

1. On an up-to-date `master` with a clean tree, run `nox -s make_release_commit -- X.Y.Z` (no leading
   `v`). It runs `towncrier build`, which writes the `## [X.Y.Z]` section of `CHANGELOG.md` and deletes
   the fragments.
2. Update the usage block in `README.md` from `b2 --help`:
   `COLUMNS=4000 b2 --help | awk '/^usages:/ {p=1; next} p {sub(/^ */, "", $0); print}'`
3. Commit as `release X.Y.Z`. Today it is pushed to `master` directly, as `make_release_commit` prints;
   `intent/review-process/intent.md` proposes a PR instead, so CI and a maintainer see it before the tag.
4. Once it is on `master` and CI there is green, tag that commit and push the tag:
   `git tag vX.Y.Z && git push upstream vX.Y.Z`.
5. Watch `cd.yml` to the end. The release is done when every job passed.

## What `cd.yml` publishes

| Job | Output |
| --- | --- |
| `deploy` | sdist and wheel on PyPI (skipped for pre-release tags such as `v5.1.0rc1`), GitHub release with the changelog section as its body |
| `deploy-linux-bundle` | Standalone Linux binaries (x86_64 and arm64), with hashes, attached to the GitHub release |
| `deploy-windows-bundle` | Signed Windows binary, with hashes, attached to the GitHub release |
| `deploy-docker` | `backblazeit/b2` Docker image (not for pre-releases) |

Runner images in `cd.yml` must match the bundle jobs in `ci.yml`, so what was tested is what ships.

## Staged releases

New or risky behavior can ship first as a pre-release tag (`vX.Y.ZrcN`, `aN`, `bN` or `devN`). `cd.yml`
then publishes the GitHub release and standalone binaries but skips PyPI and Docker, so early users can try
it before `pip install b2` picks it up. ApiVer is the CLI's flag mechanism: incompatible behavior goes to
the next `b2vN`, and scripts pinned to an older one keep the old behavior.

## If something goes wrong

- A failed `cd.yml` job can be re-run from the Actions tab without a new tag.
- To roll back a bad release in one step, yank it on PyPI (`pip` then skips it unless pinned) and mark the
  GitHub release as a pre-release; then fix forward.
- After a post-release fix, ask which layer should have caught it (a test, `REVIEW.md`, the AI review) and
  add the case to [doc/reviewer-evals.md](doc/reviewer-evals.md).
- Never move or delete a pushed tag that reached PyPI; PyPI does not accept the same version twice. Fix
  forward with a patch release.
- A security fix ships as a patch release with a `fixed` fragment and a GitHub security advisory (see
  [SECURITY.md](SECURITY.md)).
