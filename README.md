# B2 Command Line Tool
&nbsp;[![Continuous Integration](https://github.com/Backblaze/B2_Command_Line_Tool/workflows/Continuous%20Integration/badge.svg)](https://github.com/Backblaze/B2_Command_Line_Tool/actions?query=workflow%3A%22Continuous+Integration%22)&nbsp;[![License](https://img.shields.io/pypi/l/b2.svg?label=License)](https://pypi.python.org/pypi/b2)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/b2.svg?label=python%20versions)](https://pypi.python.org/pypi/b2)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/b2.svg?label=PyPI%20version)](https://pypi.python.org/pypi/b2)&nbsp;[![Docs](https://readthedocs.org/projects/b2-command-line-tool/badge/?version=master)](https://b2-command-line-tool.readthedocs.io/en/master/?badge=master)




The command-line tool that gives easy access to all of the capabilities of B2 Cloud Storage.

This program provides command-line access to the B2 service.

# Documentation

The latest documentation is available on [Read the Docs](https://b2-command-line-tool.readthedocs.io/).

# Installation

Stand-alone binaries are available for Linux, MacOS and Windows - this is the most straightforward way to use the 
command-line tool and is sufficient in most use cases. The latest versions are available for download from the 
[Releases page](https://github.com/Backblaze/B2_Command_Line_Tool/releases).

Alternatively, you can install with:

    pip install b2

# Usage

    b2 authorize-account [-h]  [applicationKeyId] [applicationKey]
    b2 cancel-all-unfinished-large-files [-h] bucketName
    b2 cancel-large-file [-h] fileId
    b2 clear-account [-h]
    b2 copy-file-by-id [-h] [--fetchMetadata] [--contentType CONTENTTYPE] [--range RANGE] [--info INFO | --noInfo] [--destinationServerSideEncryption {SSE-B2,SSE-C}] [--destinationServerSideEncryptionAlgorithm {AES256}] [--sourceServerSideEncryption {SSE-C}] [--sourceServerSideEncryptionAlgorithm {AES256}] [--fileRetentionMode {compliance,governance}] [--retainUntil TIMESTAMP] [--legalHold {on,off}] sourceFileId destinationBucketName b2FileName
    b2 create-bucket [-h] [--bucketInfo BUCKETINFO] [--corsRules CORSRULES] [--lifecycleRules LIFECYCLERULES] [--fileLockEnabled] [--defaultServerSideEncryption {SSE-B2,none}] [--defaultServerSideEncryptionAlgorithm {AES256}] bucketName bucketType
    b2 create-key [-h] [--bucket BUCKET] [--namePrefix NAMEPREFIX] [--duration DURATION] [--allCapabilities] keyName [capabilities]
    b2 delete-bucket [-h] bucketName
    b2 delete-file-version [-h] [fileName] fileId
    b2 delete-key [-h] applicationKeyId
    b2 download-file-by-id [-h] [--noProgress] [--sourceServerSideEncryption {SSE-C}] [--sourceServerSideEncryptionAlgorithm {AES256}] fileId localFileName
    b2 download-file-by-name [-h] [--noProgress] [--sourceServerSideEncryption {SSE-C}] [--sourceServerSideEncryptionAlgorithm {AES256}] bucketName b2FileName localFileName
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
    b2 ls [-h] [--long] [--json] [--versions] [--recursive] bucketName [folderName]
    b2 make-url [-h] fileId
    b2 make-friendly-url [-h] bucketName fileName
    b2 sync [-h] [--noProgress] [--dryRun] [--allowEmptySource] [--excludeAllSymlinks] [--threads THREADS] [--compareVersions {none,modTime,size}] [--compareThreshold MILLIS] [--excludeRegex REGEX] [--includeRegex REGEX] [--excludeDirRegex REGEX] [--excludeIfModifiedAfter TIMESTAMP] [--destinationServerSideEncryption {SSE-B2,SSE-C}] [--destinationServerSideEncryptionAlgorithm {AES256}] [--sourceServerSideEncryption {SSE-C}] [--sourceServerSideEncryptionAlgorithm {AES256}] [--skipNewer | --replaceNewer] [--delete | --keepDays DAYS] source destination
    b2 update-bucket [-h] [--bucketInfo BUCKETINFO] [--corsRules CORSRULES] [--lifecycleRules LIFECYCLERULES] [--defaultRetentionMode {compliance,governance,none}] [--defaultRetentionPeriod period] [--defaultServerSideEncryption {SSE-B2,none}] [--defaultServerSideEncryptionAlgorithm {AES256}] bucketName bucketType
    b2 upload-file [-h] [--noProgress] [--quiet] [--contentType CONTENTTYPE] [--minPartSize MINPARTSIZE] [--sha1 SHA1] [--threads THREADS] [--info INFO] [--destinationServerSideEncryption {SSE-B2,SSE-C}] [--destinationServerSideEncryptionAlgorithm {AES256}] [--legalHold {on,off}] [--fileRetentionMode {compliance,governance}] [--retainUntil TIMESTAMP] bucketName localFilePath b2FileName
    b2 update-file-legal-hold [-h] [fileName] fileId {on,off}
    b2 update-file-retention [-h] [--retainUntil TIMESTAMP] [--bypassGovernance] [fileName] fileId {governance,compliance,none}
    b2 version [-h]


The environment variable `B2_ACCOUNT_INFO` specifies the sqlite
file to use for caching authentication information.
The default file to use is: `~/.b2_account_info`

For more details on one command: `b2 <command> --help`

When authorizing with application keys, this tool requires that the key
have the 'listBuckets' capability so that it can take the bucket names
you provide on the command line and translate them into bucket IDs for the
B2 Storage service.  Each different command may required additional
capabilities.  You can find the details for each command in the help for
that command.

## Parallelism and the --threads parameter

Users with high performance networks, or file sets with very small files, may benefit from
increased parallelism. Experiment with using the `--threads` parameter with small values to
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

In order to see the raw request headers you may `export B2_DEBUG_HTTP=1` before calling the cli.

# Release History

Please refer to the [changelog](CHANGELOG.md).

# Developer Info

Please see our [contributing guidelines](CONTRIBUTING.md).
