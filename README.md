# B2 Command Line Tool
&nbsp;[![Continuous Integration](https://github.com/Backblaze/B2_Command_Line_Tool/workflows/Continuous%20Integration/badge.svg)](https://github.com/Backblaze/B2_Command_Line_Tool/actions?query=workflow%3A%22Continuous+Integration%22)&nbsp;[![License](https://img.shields.io/pypi/l/b2.svg?label=License)](https://pypi.python.org/pypi/b2)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/b2.svg?label=python%20versions)](https://pypi.python.org/pypi/b2)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/b2.svg?label=PyPI%20version)](https://pypi.python.org/pypi/b2)

The command-line tool that gives easy access to all of the capabilities of B2 Cloud Storage.

This program provides command-line access to the B2 service.

Version 2.0.2

# Installation

This tool can be installed with:

    pip install b2

# Usage

    b2 authorize-account [-h] [applicationKeyId] [applicationKey]
    b2 cancel-all-unfinished-large-files [-h] bucketName
    b2 cancel-large-file [-h] fileId
    b2 clear-account [-h]
    b2 copy-file-by-id [-h] [--metadataDirective {copy,replace}]
                       [--contentType CONTENTTYPE] [--range RANGE] [--info INFO]
                       sourceFileId destinationBucketName b2FileName
    b2 create-bucket [-h] [--bucketInfo BUCKETINFO] [--corsRules CORSRULES]
                     [--lifecycleRules LIFECYCLERULES]
                     bucketName bucketType
    b2 create-key [-h] [--bucket BUCKET] [--namePrefix NAMEPREFIX]
                  [--duration DURATION]
                  keyName capabilities
    b2 delete-bucket [-h] bucketName
    b2 delete-file-version [-h] [fileName] fileId
    b2 delete-key [-h] applicationKeyId
    b2 download-file-by-id [-h] [--noProgress] fileId localFileName
    b2 download-file-by-name [-h] [--noProgress]
                             bucketName b2FileName localFileName
    b2 get-account-info [-h]
    b2 get-bucket [-h] [--showSize] bucketName
    b2 get-file-info [-h] fileId
    b2 get-download-auth [-h] [--prefix PREFIX] [--duration DURATION] bucketName
    b2 get-download-url-with-auth [-h] [--duration DURATION] bucketName fileName
    b2 hide-file [-h] bucketName fileName
    b2 list-buckets [-h] [--json]
    b2 list-keys [-h] [--long]
    b2 list-parts [-h] largeFileId
    b2 list-unfinished-large-files [-h] bucketName
    b2 ls [-h] [--long] [--json] [--versions] [--recursive] [--prefix]
          bucketName [folderName]
    b2 make-url [-h] fileId
    b2 make-friendly-url [-h] bucketName fileName
    b2 sync [-h] [--noProgress] [--dryRun] [--allowEmptySource]
            [--excludeAllSymlinks] [--threads THREADS]
            [--compareVersions {none,modTime,size}] [--compareThreshold MILLIS]
            [--excludeRegex REGEX] [--includeRegex REGEX]
            [--excludeDirRegex REGEX] [--excludeIfModifiedAfter TIMESTAMP]
            [--skipNewer | --replaceNewer] [--delete | --keepDays DAYS]
            source destination
    b2 update-bucket [-h] [--bucketInfo BUCKETINFO] [--corsRules CORSRULES]
                     [--lifecycleRules LIFECYCLERULES]
                     bucketName bucketType
    b2 upload-file [-h] [--noProgress] [--quiet] [--contentType CONTENTTYPE]
                   [--minPartSize MINPARTSIZE] [--sha1 SHA1] [--threads THREADS]
                   [--info INFO]
                   bucketName localFilePath b2FileName
    b2 version [-h]


The environment variable B2_ACCOUNT_INFO specifies the sqlite
file to use for caching authentication information.
The default file to use is: ~/.b2_account_info

For more details on one command: b2 help <command>

When authorizing with application keys, this tool requires that the key
have the 'listBuckets' capability so that it can take the bucket names
you provide on the command line and translate them into bucket IDs for the
B2 Storage service.  Each different command may required additional
capabilities.  You can find the details for each command in the help for
that command.

## Parallelism and the --threads parameter

Users with high performance networks, or file sets with very small files, may benefit from
increased parallelism. Experiment with using the --threads parameter with small values to
determine if there are benefits.

Note that using multiple threads will usually be detrimental to the other users on your network.

# Contrib

## bash completion

You can find a [bash completion](https://www.gnu.org/software/bash/manual/html_node/Programmable-Completion.html#Programmable-Completion)
script in the `contrib` directory. See [this](doc/bash_completion.md) for installation instructions.

## detailed logs

Verbose logs to stdout can be enabled with the `--verbose` flag.

A hidden flag `--debugLogs` can be used to enable logging to a `b2_cli.log` file (with log rotation at midnight) in current working directory. Please take care to not launch the tool from the directory that you are syncing, or the logs will get synced to the remote server (unless that is really what you want to do).

For advanced users, a hidden option `--logConfig <filename.ini>` can be used to enable logging in a user-defined format and verbosity. An example log configuration can be found [here](contrib/debug_logs.ini).

# Release History

## Not released yet

* Drop Python 2 support :tada: (for old systems you can now use the [binary distribution](https://www.backblaze.com/b2/docs/quick_command_line.html))

## 2.0.2 (2020-07-15)

Changes:

* Add `--environment` internal parameter for `authorize-account`

## 2.0.0 (2020-06-25)

Changes:

* Switch to b2sdk api version v1: remove output of `delete-bucket`
* Use b2sdk >1.1.0: add large file server-side copy
* Remove the ability to import b2sdk classes through b2cli (please use b2sdk directly)
* Remove official support for python 3.4
* Add official support for python 3.8
* Add `make-friendly-url` command
* Add `--excludeIfModifiedAfter` parameter for `sync`
* Switch option parser to argparse: readthedocs documentation is now generated automatically
* Add `--json` parameter to `ls` and `list-buckets`
* Remove `list-file-names` command. Use `ls --recursive --json` instead
* Remove `list-file-versions` command. Use `ls --recursive --json --versions` instead
* Normalize output indentation level to 4 spaces
* Introduce bundled versions of B2 CLI for Linux, Mac OS and Windows

## 1.4.2 (2019-10-03)

Changes:

* Add `prefix` parameter to `list-file-names` and `list-file-versions`
* Make parameters of `list-file-names` and `list-file-versions` optional (use an empty string like this: `""`)
* (b2sdk) Fix sync when used with a key restricted to filename prefix
* When authorizing with application keys, optional application key ID and 
  application key can be added using environment variables 
  B2_APPLICATION_KEY_ID and B2_APPLICATION_KEY respectively.
* Add support for (server-side) copy-file command

## 1.4.0 (April 25, 2019)

Changes:

* Renaming accountId for authentication to application key Id
    Note: this means account Id is still backwards compatible,
    only the terminology has changed.
* Most of the code moved to b2sdk [repository](https://github.com/Backblaze/b2-sdk-python) and [package](https://pypi.org/project/b2sdk/)
* Deprecation warning added for imports of sdk classes from cli package
* (b2sdk) Fix transferer crashing on empty file download attempt
* (b2sdk) Enable retries of non-transfer operations
* (b2sdk) Enable continuation of download operations
* (b2sdk) Support for python 3.7

## 1.3.8 (December 6, 2018)

New features:

* New `--excludeAllSymlinks` option for `sync`.
* Faster downloading of large files using multiple threads and bigger buffers.

Bug fixes:

* Fixed doc for cancel-all-unfinished-large-files

## 1.3.6 (August 21, 2018)

Bug fixes:

* Fix auto-reauthorize for application keys.
* Fix problem with bash auto-completion module.
* Fix (hopefully) markdown display in PyPI.

## 1.3.4 (August 10, 2018)

Bug fixes:

* Better documentation for authorize-account command.
* Fix error reporting when using application keys
* Fix auth issues with bucket-restricted application keys.

## 1.3.2 (July 28, 2018)

Bug fixes:

* Tests fixed for Python 3.7
* Add documentation about what capabilites are required for different commands.
* Better error messages for authorization problems with application keys.

## 1.3.0 (July 20, 2018)

New features:

* Support for [application keys](https://www.backblaze.com/b2/docs/application_keys.html).
* Support for Python 3.6
* Drop support for Python 3.3 (`setuptools` no longer supports 3.3)

Bug fixes:

* Fix content type so markdown displays properly in PyPI
* The testing package is called `test`, not `tests`

Internal upgrades:

* Faster and more complete integration tests

## 1.2.0 (July 6, 2018)

New features:

* New `--recursive` option for ls
* New `--showSize` option for get-bucket
* New `--excludeDirRegex` option for sync

And some bug fixes:

* Include LICENSE file in the source tarball. Fixes #433
* Test suite now runs as root (fixes #427)
* Validate file names before trying to upload
* Fix scaling problems when syncing large numbers of files
* Prefix Windows paths during sync to handle long paths (fixes #265)
* Check if file to be synced is still accessible before syncing (fixes #397)

## 1.1.0 (November 30, 2017)

Just one change in this release:

* Add support for CORS rules in `create-bucket` and `update-bucket`.  `get-bucket` will display CORS rules.

## 1.0.0 (November 9, 2017)

This is the same code as 0.7.4, with one incompatible change:

* Require `--allowEmptySource` to sync from empty directory, to help avoid accidental deletion of all files.

## 0.7.4 (November 9, 2017)

New features:

* More efficient uploads by sending SHA1 checksum at the end.

Includes a number of bug fixes:

* File modification times are set correctly when downloading.
* Fix an off-by-one issue when downloading a range of a file (affects library, but not CLI).
* Better handling of some errors from the B2 service.

# Developer Info

We encourage outside contributors to perform changes on our codebase. Many such changes have been merged already. In order to make it easier to contribute, core developers of this project:

* provide guidance (through the issue reporting system)
* provide tool assisted code review (through the Pull Request system)
* maintain a set of unit tests
* maintain a set of integration tests (run with a production cloud)
* maintain development automation tools using [nox](https://github.com/theacodes/nox) that can easily:
   * format the code using [yapf](https://github.com/google/yapf)
   * runs linters to find subtle/potential issues with maintainability
   * run the test suite on multiple Python versions using [pytest](https://github.com/pytest-dev/pytest)
* maintain Continuous Integration (by using GitHub Actions) that:
   * runs all sorts of linters
   * checks if the Python distribution can be built
   * runs all tests on a matrix of 6 versions of Python (including pypy) and 3 operating systems (Linux, Mac OS X and Windows)
   * checks if the documentation can be built properly
* maintain other Continuous Integration tools (coverage tracker)

You'll need to have [nox](https://github.com/theacodes/nox) installed:

* `pip install nox`

With `nox`, you can run different sessions (default are `lint` and `test`):

* `format` -> Format the code.
* `lint` -> Run linters.
* `test` (`test-3.5`, `test-3.6`, `test-3.7`, `test-3.8`) -> Run test suite.
* `cover` -> Perform coverage analysis.
* `build` -> Build the distribution.
* `deploy` -> Deploy the distribution to the PyPi.
* `doc` -> Build the documentation.
* `doc_cover` -> Perform coverage analysis for the documentation.

For example:

    $ nox -s format
    nox > Running session format
    nox > Creating virtual environment (virtualenv) using python3.8 in .nox/format
    ...

    $ nox --no-venv -s format
    nox > Running session format
    ...

    $ nox -s test
    nox > Running session test-3.5
    ...
    nox > Running session test-3.6
    ...
    nox > Running session test-3.7
    ...
    nox > Running session test-3.8
    ...

    $ nox -s test-3.8
    nox > Running session test-3.8
    nox > Re-using existing virtual environment at .nox/test-3-8.
    ...

Session `test` can run on many python versions.
In order to do it, given Python interpreter must be installed in the operating system or via [pyenv](https://github.com/pyenv/pyenv).

Sessions other than `test` use the latest stable Python. You can change it:

    export PYTHON_DEFAULT_VERSION=3.7

The integration tests need two environment variables:

    export TEST_APPLICATION_KEY=your_app_key
    export TEST_APPLICATION_KEY_ID=your_app_key_id

We marked the places in the code which are significantly less intuitive than others in a special way. To find them occurrences, use `git grep '*magic*'`.
