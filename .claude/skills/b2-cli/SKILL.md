---
name: b2-cli
description: Use this skill when operating the b2 CLI to manage Backblaze B2 files, buckets, or application keys safely.
---

# Use the B2 CLI

## Install

Choose one installation method.

- Use Homebrew: `brew install b2-tools`.
- Download a Linux or Windows binary from the [releases](https://github.com/Backblaze/B2_Command_Line_Tool/releases).
- Install the Python package: `pip install b2`.
- Use Docker: `backblazeit/b2:latest`.

Check the installed version.

```sh
b2 version
docker run --rm backblazeit/b2:latest b2v5 version
```

## Authenticate

Set `B2_APPLICATION_KEY_ID` and `B2_APPLICATION_KEY` in the environment through a secure credential source.
Never pass the application key secret as an argument.
Never print, log, echo, or commit the secret.
Do not use the master key for routine work.

Choose only the capabilities required by the task.
Read each command's `--help` for its required capabilities.
Use `b2 key create` to restrict a routine key to one bucket and a file-name prefix.
Set a short positive `--duration` in seconds.
Ensure the issuing key can create keys and resolve the bucket name.

Capture the new key in a new private file outside the repository.
Replace `/secure/path/` with an existing private directory.
The key secret is shown once.
Do not use `-q` or `--quiet` with `key create`: quiet mode suppresses the secret ([#1291](https://github.com/Backblaze/B2_Command_Line_Tool/issues/1291)).
Never display the capture file.
Deliver the secret through a secure credential channel.
Delete the capture file when it is no longer needed.

```sh
umask 077
set -C
b2v5 key create --bucket example-bucket --name-prefix incoming/ --duration 3600 upload-hour listBuckets,writeFiles > /secure/path/b2-key-output
```

## Choose an interface

Use `b2v5` in scripts and automation.
Use `b2` interactively.
Treat ApiVer as the compatibility boundary.
The unversioned command can select a different stable ApiVer in a future release.

## Read output and exit codes

Follow the [scripting contract](https://github.com/Backblaze/B2_Command_Line_Tool/blob/master/doc/source/scripting.rst).
Check the exit status before consuming a complete result.

- Expect JSON by default from `account authorize`, `account get`, `bucket get`, `file info`, `file hide`, `file unhide`, and `file server-side-copy`.
- Request JSON with `--json` for `ls`, `bucket list`, and `bucket notification-rule list`.
- Expect text from `key create`, `key delete`, and `key list`; `key list` has no `--json`.
- Expect two URL lines followed by JSON from `file upload`.
- Expect file bytes from `file cat` and `file download` with destination `-`.
- Expect download metadata and completion messages on stdout when downloading to a local path.
- Treat exit 0 as success or explicit help; a handled broken output pipe also returns 0.
- Treat exit 1 as a command failure; partial work may already have happened.
- Treat exit 2 as an argument-parser usage error.

Capture stdout and stderr separately.
Expect results on stdout and general handled errors on stderr.
Expect upload and download progress on stderr.
Expect `rm` and `sync` operation-error reports and progress on stdout.
Use `--no-progress` where supported to suppress progress displays.
Do not treat `--quiet` as a guarantee that data results survive.

## Confirm destructive actions

Show the planned action and obtain the user's confirmation before deleting or overwriting data.
Require confirmation for `rm`, `sync --delete`, `sync --keep-days`, and `sync --replace-newer`.
Require confirmation for `bucket delete`, `key delete`, `file hide`, `file unhide`, and `file update`.
Confirm uploads that replace an existing name and downloads that overwrite a local file.
Preview `rm` and `sync` with `--dry-run` before asking for confirmation.

```sh
b2v5 rm --dry-run b2://example-bucket/incoming/example.txt
b2v5 sync --dry-run --delete ./incoming b2://example-bucket/incoming/
```

## Upload, download, list, and sync

Replace the example bucket and paths with the user's confirmed targets.
Use a bucket name and a remote file name for upload; use B2 URIs for download, listing, and sync.
Check the exit status after each operation.

```sh
b2v5 file upload --no-progress example-bucket ./example.txt incoming/example.txt
b2v5 file download --no-progress b2://example-bucket/incoming/example.txt ./downloaded-example.txt
b2v5 ls --json b2://example-bucket/incoming/
b2v5 sync --dry-run ./incoming b2://example-bucket/incoming/
```

Review the sync preview and obtain confirmation for overwrites before running the actual sync.

```sh
b2v5 sync --no-progress ./incoming b2://example-bucket/incoming/
```
