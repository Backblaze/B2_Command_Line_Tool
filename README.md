# B2 Command Line Tool&nbsp;[![Travis CI](https://img.shields.io/travis/Backblaze/B2_Command_Line_Tool/master.svg?label=Travis%20CI)](https://travis-ci.org/Backblaze/B2_Command_Line_Tool)&nbsp;[![License](https://img.shields.io/pypi/l/b2.svg?label=License)](https://pypi.python.org/pypi/b2)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/b2.svg?label=python%20versions)](https://pypi.python.org/pypi/b2)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/b2.svg?label=PyPI%20version)](https://pypi.python.org/pypi/b2)

The command-line tool that gives easy access to all of the capabilities of B2 Cloud Storage.

This program provides command-line access to the B2 service.

Version 1.0.0

# Installation

This tool can be installed with:

    pip install b2

If you see a message saying that the `six` library cannot be installed, which
happens if you're installing with the system python on OS X El Capitan, try
this:

    pip install --ignore-installed b2

# Usage

    b2 authorize-account [<accountId>] [<applicationKey>]
    b2 cancel-all-unfinished-large-files <bucketName>
    b2 cancel-large-file <fileId>
    b2 clear-account
    b2 create-bucket [--bucketInfo <json>] [--lifecycleRules <json>] <bucketName> [allPublic | allPrivate]
    b2 delete-bucket <bucketName>
    b2 delete-file-version [<fileName>] <fileId>
    b2 download-file-by-id [--noProgress] <fileId> <localFileName>
    b2 download-file-by-name [--noProgress] <bucketName> <fileName> <localFileName>
    b2 get-account-info
    b2 get-bucket <bucketName>
    b2 get-download-auth [--prefix <fileNamePrefix>] [--duration <durationInSeconds>] <bucketName>
    b2 get-download-url-with-auth [--duration <durationInSeconds>] <bucketName> <fileName>
    b2 get-file-info <fileId>
    b2 help [commandName]
    b2 hide-file <bucketName> <fileName>
    b2 list-buckets
    b2 list-file-names <bucketName> [<startFileName>] [<maxToShow>]
    b2 list-file-versions <bucketName> [<startFileName>] [<startFileId>] [<maxToShow>]
    b2 list-parts <largeFileId>
    b2 list-unfinished-large-files <bucketName>
    b2 ls [--long] [--versions] <bucketName> [<folderName>]
    b2 make-url <fileId>
    b2 rm [--report] [--versions] <bucketName> <glob>
    b2 sync [--delete] [--keepDays N] [--skipNewer] [--replaceNewer] \
        [--compareVersions <option>] [--compareThreshold N] \
        [--threads N] [--noProgress] [--dryRun ] [--allowEmptySource ] \
        [--excludeRegex <regex> [--includeRegex <regex>]] \
        <source> <destination>
    b2 update-bucket [--bucketInfo <json>] [--lifecycleRules <json>] <bucketName> [allPublic | allPrivate]
    b2 upload-file [--sha1 <sha1sum>] [--contentType <contentType>] \
        [--info <key>=<value>]* [--minPartSize N] \
        [--noProgress] [--threads N] <bucketName> <localFilePath> <b2FileName>
    b2 version

    For more details on one command: b2 help <command>

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
* maintain a set of integration tests (run with a production cloud)
* maintain a set of (well over a hundred) unit tests
* automatically run unit tests on 14 versions of python (including osx, Jython and pypy)
* format the code automatically using [yapf](https://github.com/google/yapf)
* use static code analysis to find subtle/potential issues with maintainability
* maintain other Continous Integration tools (coverage tracker)

You'll need to have these packages installed:

* nose
* pyflakes
* six
* yapf

There is a `Makefile` with a rule to run the unit tests using the currently active Python:

    make test

To test in multiple python virtual environments, set the enviroment variable `PYTHON_VIRTUAL_ENVS`
to be a space-separated list of their root directories.  When set, the makefile will run the
unit tests in each of the environments.

Before checking in, use the `pre-commit.sh` script to check code formatting, run
unit tests, run integration tests etc.

The integration tests need a file in your home directory called `.b2_auth`
that contains two lines with nothing on them but your account ID and application key:

     accountId
     applicationKey

We marked the places in the code which are significantly less intuitive than others in a special way. To find them occurrences, use `git grep '*magic*'`.
