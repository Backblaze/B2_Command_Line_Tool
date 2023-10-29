# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.12.0] - 2023-10-28

### Fixed
* `--quiet` now will implicitly set `--noProgress` option as well
* pypy integration tests

### Infrastructure
* Use stable Python 3.12 in CI
* Fix readthedocs build by updating to v2 configuration schema

## [3.11.0] - 2023-10-04

### Added
* Add `--quiet` option to all commands to suppress all messages printed to stdout & stderr

### Changed
* Improve `--quiet` and `--profile` options documentation mentions, while suppressing them in `--help` output

### Infrastructure
* Fix gathering licenses of typeshed libraries
* Fix spellcheck erroring out on LICENSE file

## [3.10.1] - 2023-09-27

### Fixed
* Fix lifecycle rules being cleared after using `update-bucket` command if not explicitly set again.
* Fix missing key ID for large file encrypted with SSE-C (fixed by `b2sdk` update)

### Infrastructure
* Fix bad version number generation in CD

## [3.10.0] - 2023-09-10

### Added
* docker tests and pushing the official docker image on release
* Add ability to upload from an unbound source such as standard input or a named pipe
* --bypassGovernance option to delete_file_version
* Declare official support of Python 3.12
* Cache-Control option when uploading files
* Add `--lifecycleRule` to `create-bucket` and `update-bucket` and deprecate `--lifecycleRules` argument
* Add extra dependencies for better UX, installable with `pip install b2[full]`
* Add s3 endpoint to `get-account-info`

### Deprecated
* Support of `-` as a valid filename in `upload-file` command. In future `-` will be an alias for standard input.

### Changed
* Better help text for --corsRules
* if `--threads` is not explicitly set, number of threads is no longer guaranteed to be 10 

### Infrastructure
* Remove unsupported PyPy 3.7 from tests matrix and add PyPy 3.10 instead
* Autocomplete integration tests will now work properly even if tested package has not been installed
* Automatically set copyright date when generating the docs
* Increase timeout time in autocomplete tests to accommodate slower CI environments
* Update pyinstaller to fix Linux Bundle build
* Replace `pyflakes` with `ruff` for linting
* Make dependency version pinning less restrictive
* Fix tests by making mocks compatible with latest `b2sdk` version
* Fix readthedocs build

### Fixed
* Fast rm sometimes failing due to a rare race condition
* Fix UnicodeEncodeError in non-Unicode terminals by prioritizing stdout encoding
* When listing licenses in `license` command only show licenses of `b2` and its dependencies
* Fix license command failing on Windows when non-UTF8 encoding is the default

## [3.9.0] - 2023-04-28

### Added
* Support for custom file upload timestamp

### Infrastructure
* Limit GitHub CI workload by running most integration tests only against edge versions of supported Python versions
* Add a direct dependency from tqdm

## [3.8.0] - 2023-03-23

### Added
* Add `install-autocomplete` command for installing shell autocompletion (currently only `bash` is supported)

### Fixed
* Hitting the download endpoint twice in some cases

### Infrastructure
* GitHub CD builds and uploads an official B2 CLI image to docker hub
* Disable changelog verification for dependabot PRs

## [3.7.1] - 2023-02-08

### Fixed
* Remove unnecessary printing options from `rm`
* Clarify that `--recursive` is required when `--withWildcard` is used
* Adjust description of `rm`

### Infrastructure
* Remove macos stand-alone binary from CI/CD

## [3.7.0] - 2023-02-07

### Added
* Add `--incrementalMode` to `sync` and `upload-file`
* Add `license` command for printing licenses of b2 and libraries
* Add wildcard support for the `ls` command
* Add `rm` command

### Fixed
* Stop using b2sdk.v1 in arg_parser.py
* Fix issues when running commands on Python 3.11
* Fix tests after changes introduced in b2sdk 1.19.0
* `rm` can handle any number of files

### Infrastructure
* GitHub CI got checkout action updated to v3 and setup-python to v4
* Ensured that changelog validation only happens on pull requests
* GitHub CI uses GITHUB_OUTPUT instead of deprecated set-output
* Releases now feature digests of each file
* Change default Python version in CI/CD to 3.11
* Temporary marking all directories as `safe.directory` inside CI/CD when bundling

## [3.6.0] - 2022-09-20

### Added
* Add `replication-delete` command
* Add `replication-pause` command
* Add `replication-status` command
* Add `replication-unpause` command
* Add `--include-existing-files` to `replication-setup`
* Add `--max-streams` parameter to download commands
* Add `--fileLockEnabled` switch to `update-bucket` subcommand

### Fixed
* Fix `replication-setup` default priority setter

### Infrastructure
* Fix warnings in tests
* Fix `test_keys` unit test after changes in b2sdk
* Fix running tests on the CI with the latest SDK from the master branch

## [3.5.0] - 2022-07-27

As in 3.4.0, replication support may be unstable, however no backward-incompatible
changes are currently planned.
This version is pinned strictly to `b2-sdk-python==1.17.3` for the same reason.

### Added
* Add `--write-buffer-size` parameter
* Add `--skip-hash-verification` parameter

### Changed
* Minimum MacOS version from 10.15 to 11.0

### Infrastructure
* Try not to crash tests due to bucket name collision
* Fix replication integration tests
* Fix leaking buckets in integration tests
* Limit number of workers for integration tests to 1 for now
* Make integration tests remove buckets only based on name, not based on creation time
* Add dependabot configuration

## [3.4.0] - 2022-05-04

This release contains a preview of replication support. It allows for basic usage
of B2 replication feature (currently in closed beta). Until this notice is removed,
the interface of replication related functionality should be not considered as public
API (as defined by SemVer).
This version is pinned strictly to `b2-sdk-python==1.16.0` for the same reason.

### Added
* Add basic replication support to `create-bucket` and `update-bucket`
* Add more fields to `get-account-info` json
* Add `--replication` to `ls --long`
* Add `replication-setup` command
* Add "quick start guide" to documentation

### Changed
* Made `bucketType` positional argument to `update-bucket` optional
* Run unit tests on all CPUs

## [3.3.0] - 2022-04-20

### Added
* Add `--threads` parameter to `download-file-by-name` and `download-file-by-id`
* Add `--uploadThreads` and `--downloadThreads` parameters to `sync`
* Add `--profile` switch support
* Add `applicationKeyId` and `isMasterKey` to the output of `get-account-info`

### Changed
* Rename `--threads` parameter for `--sync` to `--syncThreads`

### Fixed
* Fix license header checker on Windows
* Fix `UnicodeEncodeError` after successful SSE-C download on a non-utf8 terminal (#786)

### Removed
* Remove official support for python 3.5
* Remove official support for python 3.6

## [3.2.1] - 2022-02-23

### Fixed
* Fix setting permissions for local sqlite database (thanks to Jan Schejbal for responsible disclosure!)

## [3.2.0] - 2021-12-23

### Added
* Add compatibility support for arrow >= 1.0.2 on newer Python versions while
  continuing to support Python 3.5

### Fixed
* Fallback to `ascii` decoder when printing help in case the locales are not properly set
* Apply the value of `--threads` parameter to `sync` downloader threads

## [3.1.0] - 2021-11-02

### Added
* Add `--allCapabilities` to `create-key`
* Add support for Python 3.10

### Fixed
* Fix testing bundle in CI for a new `staticx` version

## [3.0.3] - 2021-09-27

### Fixed
* Fix pypy selector in CI
* Fix for static linking of Linux binary (CD uses python container)

## [3.0.2] - 2021-09-17

### Added
* Sign Windows binary

### Changed
* Download instruction in README.md (wording suggested by https://github.com/philh7456)
* Make Linux binary statically linked

## [3.0.1] - 2021-08-09

### Fixed
* logs from all loggers (in dependencies too) brought back

## [3.0.0] - 2021-08-07

### Added
* Add possibility to change realm during integration tests
* Add possibility to install SDK from local folder instead of pypi when running tests
* Add full support of establishing file metadata when copying, with either source or target using SSE-C
* Add `--noInfo` option to `copy-file-by-id`
* Integration test for checking if `bad_bucket_id` error code is returned

### Fixed
* Fix integration tests on non-production environments
* Fix warnings thrown by integration tests
* delete-key unit test adjusted to a less mocked simulator
* Fix integration test cleanup
* Representing encryption-related metadata in buckets and file versions is now consistent

### Changed
* CLI now uses `b2sdk.v2`
* Downloading files prints file metadata as soon as the download commences (not when it finishes)
* New way of establishing location of the SQLite cache file, using `XDG_CONFIG_HOME` env var
* Downloaded file's metadata is complete and is displayed before the file is downloaded, a `Download finished` message
  is issued at the end
* `contentLength` changed to `size` where appropriate
* Log configuration: stack traces are not printed in case of errors by default, `--verbose` changes that
* Log configuration arguments behaviour altered: `--logConfig` is exclusive with `--verbose` and `--debugLogs`
* Log configuration arguments behaviour altered: `--verbose` and `--debugLogs` can be used at the same time
  (and they will both be taken into account)

### Removed
* Support of `--metadataDirective` argument in `copy-file-by-id` (the `metadataDirective` sent to B2 cloud is
  detected automatically)

## [2.5.1] - 2021-08-06

* `SRC_LAST_MODIFIED_MILLIS` import fix

## [2.5.0] - 2021-05-22

### Added
* Add integration test for sync within one bucket with different encryption
* Notarize OSX binary
* File lock arguments and new commands

### Fixed
* Fixed breaking integration test case
* Add zoneinfo to the Windows bundle
* Fixed unit tests failing on new attributes of FileVersionInfo
* Removing old buckets in integration tests
* Bucket name entropy in tests increased

## [2.4.0] - 2021-04-22

### Added
* Sign OSX binary
* Add support for SSE-C server-side encryption mode

### Fixed
* Exclude packages inside the test package when installing

## [2.3.0] - 2021-03-25

### Added
* Add support for SSE-B2 server-side encryption mode

### Fixed
* Pin `setuptools-scm<6.0` as `>=6.0` doesn't support Python 3.5
* Fix boot speed regression caused by the `rst2ansi` invocations

## [2.2.0] - 2021-03-15

### Added
* Option to automatically authorize account when running commands other than `authorize-account` via
  `B2_APPLICATION_KEY_ID` and `B2_APPLICATION_KEY` env vars

### Changed
* Improve setup and teardown for the integration tests
* Use `setuptools-scm` for versioning
* Improve CLI and RTD descriptions of the commands
* Add upper version limit for arrow dependency, because of a breaking change

### Fixed
* Fix for the Windows bundled version
* Fix docs autogen

## [2.1.0] - 2020-11-03

### Added
* Add support for Python 3.9
* Add a possibility to append a string to the User-Agent via `B2_USER_AGENT_APPEND` env

### Changed
* Update `b2 sync` usage text for bucket-to-bucket sync

### Removed
* Drop Python 2 support :tada: (for old systems you can now use the [binary distribution](https://www.backblaze.com/b2/docs/quick_command_line.html))
* Remove `--prefix` from `ls` (it didn't really work, use `folderName` argument)
* Clean up legacy code (`CliBucket`, etc.)

### Fixed
* Fix docs generation in CI
* Correct names of the arguments in `b2 create-key` usage text

## [2.0.2] - 2020-07-15

### Added
* Add `--environment` internal parameter for `authorize-account`

## [2.0.0] - 2020-06-25

### Added
* Add official support for python 3.8
* Add `make-friendly-url` command
* Add `--excludeIfModifiedAfter` parameter for `sync`
* Add `--json` parameter to `ls` and `list-buckets`
* Introduce bundled versions of B2 CLI for Linux, Mac OS and Windows

### Changed
* Switch to b2sdk api version v1: remove output of `delete-bucket`
* Use b2sdk >1.1.0: add large file server-side copy
* Switch option parser to argparse: readthedocs documentation is now generated automatically
* Normalize output indentation level to 4 spaces

### Removed
* Remove the ability to import b2sdk classes through b2cli (please use b2sdk directly)
* Remove official support for python 3.4
* Remove `list-file-names` command. Use `ls --recursive --json` instead
* Remove `list-file-versions` command. Use `ls --recursive --json --versions` instead

## [1.4.2] - 2019-10-03

### Added
* Add `prefix` parameter to `list-file-names` and `list-file-versions`
* Add support for (server-side) copy-file command

### Changed
* Make parameters of `list-file-names` and `list-file-versions` optional (use an empty string like this: `""`)
* (b2sdk) Fix sync when used with a key restricted to filename prefix
* When authorizing with application keys, optional application key ID and
  application key can be added using environment variables
  B2_APPLICATION_KEY_ID and B2_APPLICATION_KEY respectively.

## [1.4.0] - 2019-04-25

### Added
* (b2sdk) Support for python 3.7

### Changed
* Renaming accountId for authentication to application key Id
    Note: this means account Id is still backwards compatible,
    only the terminology has changed.
* Most of the code moved to b2sdk [repository](https://github.com/Backblaze/b2-sdk-python) and [package](https://pypi.org/project/b2sdk/)
* (b2sdk) Fix transferer crashing on empty file download attempt
* (b2sdk) Enable retries of non-transfer operations
* (b2sdk) Enable continuation of download operations

### Deprecated
* Deprecation warning added for imports of sdk classes from cli package

## [1.3.8] - 2018-12-06

### Added
* New `--excludeAllSymlinks` option for `sync`.
* Faster downloading of large files using multiple threads and bigger buffers.

### Fixed
* Fixed doc for cancel-all-unfinished-large-files

## [1.3.6] - 2018-08-21

### Fixed
* Fix auto-reauthorize for application keys.
* Fix problem with bash auto-completion module.
* Fix (hopefully) markdown display in PyPI.

## [1.3.4] - 2018-08-10

### Fixed
* Better documentation for authorize-account command.
* Fix error reporting when using application keys
* Fix auth issues with bucket-restricted application keys.

## [1.3.2] - 2018-07-28

### Fixed
* Tests fixed for Python 3.7
* Add documentation about what capabilities are required for different commands.
* Better error messages for authorization problems with application keys.

## [1.3.0] - 2018-07-20

### Added
* Support for [application keys](https://www.backblaze.com/b2/docs/application_keys.html).
* Support for Python 3.6
* Drop support for Python 3.3 (`setuptools` no longer supports 3.3)

### Changed
* Faster and more complete integration tests

### Fixed
* Fix content type so markdown displays properly in PyPI
* The testing package is called `test`, not `tests`

## [1.2.0] - 2018-07-06

### Added
* New `--recursive` option for ls
* New `--showSize` option for get-bucket
* New `--excludeDirRegex` option for sync

### Fixed
* Include LICENSE file in the source tarball. Fixes #433
* Test suite now runs as root (fixes #427)
* Validate file names before trying to upload
* Fix scaling problems when syncing large numbers of files
* Prefix Windows paths during sync to handle long paths (fixes #265)
* Check if file to be synced is still accessible before syncing (fixes #397)

## [1.1.0] - 2017-11-30

### Added
* Add support for CORS rules in `create-bucket` and `update-bucket`.  `get-bucket` will display CORS rules.

### Fixed
* cleanup in integration tests works

## [1.0.0] - 2017-11-09

### Added
* Require `--allowEmptySource` to sync from empty directory, to help avoid accidental deletion of all files.

## [0.7.4] - 2017-11-09

### Added
* More efficient uploads by sending SHA1 checksum at the end.

### Fixed
* File modification times are set correctly when downloading.
* Fix an off-by-one issue when downloading a range of a file (affects library, but not CLI).
* Better handling of some errors from the B2 service.

[Unreleased]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.12.0...HEAD
[3.12.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.11.0...v3.12.0
[3.11.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.10.1...v3.11.0
[3.10.1]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.10.0...v3.10.1
[3.10.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.9.0...v3.10.0
[3.9.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.8.0...v3.9.0
[3.8.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.7.1...v3.8.0
[3.7.1]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.7.0...v3.7.1
[3.7.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.6.0...v3.7.0
[3.6.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.5.0...v3.6.0
[3.5.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.4.0...v3.5.0
[3.4.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.3.0...v3.4.0
[3.3.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.2.1...v3.3.0
[3.2.1]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.2.0...v3.2.1
[3.2.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.1.0...v3.2.0
[3.1.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.0.3...v3.1.0
[3.0.3]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.0.2...v3.0.3
[3.0.2]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.0.1...v3.0.2
[3.0.1]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v3.0.0...v3.0.1
[3.0.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v2.5.1...v3.0.0
[2.5.1]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v2.5.0...v2.5.1
[2.5.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v2.4.0...v2.5.0
[2.4.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v2.3.0...v2.4.0
[2.3.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v2.2.0...v2.3.0
[2.2.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v2.1.0...v2.2.0
[2.1.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v2.0.2...v2.1.0
[2.0.2]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v2.0.0...v2.0.2
[2.0.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v1.4.2...v2.0.0
[1.4.2]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v1.4.0...v1.4.2
[1.4.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v1.3.8...v1.4.0
[1.3.8]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v1.3.6...v1.3.8
[1.3.6]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v1.3.4...v1.3.6
[1.3.4]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v1.3.2...v1.3.4
[1.3.2]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v1.3.0...v1.3.2
[1.3.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v0.7.4...v1.0.0
[0.7.4]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v0.7.2...v0.7.4
