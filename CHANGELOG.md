# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
* Add documentation about what capabilites are required for different commands.
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

[Unreleased]: https://github.com/Backblaze/B2_Command_Line_Tool/compare/v2.2.0...HEAD
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
