# B2 Command Line Tool

&nbsp;[![Continuous Integration](https://github.com/Backblaze/B2_Command_Line_Tool/workflows/Continuous%20Integration/badge.svg)](https://github.com/Backblaze/B2_Command_Line_Tool/actions?query=workflow%3A%22Continuous+Integration%22)&nbsp;[![License](https://img.shields.io/pypi/l/b2.svg?label=License)](https://pypi.python.org/pypi/b2)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/b2.svg?label=python%20versions)](https://pypi.python.org/pypi/b2)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/b2.svg?label=PyPI%20version)](https://pypi.python.org/pypi/b2)&nbsp;[![Docs](https://readthedocs.org/projects/b2-command-line-tool/badge/?version=master)](https://b2-command-line-tool.readthedocs.io/en/master/?badge=master)

The command-line tool that gives easy access to all of the capabilities of B2 Cloud Storage.

This program provides command-line access to the B2 service.

## Documentation

The latest documentation is available on [Read the Docs](https://b2-command-line-tool.readthedocs.io/).

## Installation

For detailed instructions on how to install the command line tool see our [quick start guide](https://www.backblaze.com/b2/docs/quick_command_line.html).

### Homebrew

[Homebrew](https://brew.sh/) is widely used in the Mac community, particularly amongst developers. We recommend using the [B2 CLI Homebrew](https://formulae.brew.sh/formula/b2-tools) formula as the quickest setup method for Mac users:

```bash
brew install b2-tools
```

### Binaries

Stand-alone binaries are available for Linux and Windows; this is the most straightforward way to use the command-line tool and is sufficient in most use cases. The latest versions are available for download from the [Releases page](https://github.com/Backblaze/B2_Command_Line_Tool/releases).

### Python Package Index

You can also install it in your Python environment ([virtualenv](https://pypi.org/project/virtualenv/) is recommended) from PyPI with:

```bash
pip install b2[full]
```

The extra dependencies improve debugging experience and, potentially, performance of `b2` CLI, but are not strictly required.
You can install the `b2` without them:

```bash
    pip install b2
```

### Docker

For a truly platform independent solution, use the official docker image: 

```bash
docker run backblazeit/b2:latest  ...
```

See examples in [Usage/Docker image](#docker-image)

### Installing from source

If installing from the repository is needed in order to e.g. check if a pre-release version resolves a bug effectively, it can be installed with:

```bash
python3 setup.py install
```

In this case of installing a pre-release, [virtualenv](https://pypi.org/project/virtualenv/) is strongly recommended.

## Usage

```bash
b2 authorize-account [-h]  [applicationKeyId] [applicationKey]
b2 cancel-all-unfinished-large-files [-h] bucketName
b2 cancel-large-file [-h] fileId
b2 clear-account [-h]
b2 copy-file-by-id [-h] [--fetchMetadata] [--contentType CONTENTTYPE] [--range RANGE] [--info INFO | --noInfo] [--destinationServerSideEncryption {SSE-B2,SSE-C}] [--destinationServerSideEncryptionAlgorithm {AES256}] [--sourceServerSideEncryption {SSE-C}] [--sourceServerSideEncryptionAlgorithm {AES256}] [--fileRetentionMode {compliance,governance}] [--retainUntil TIMESTAMP] [--legalHold {on,off}] sourceFileId destinationBucketName b2FileName
b2 create-bucket [-h] [--bucketInfo BUCKETINFO] [--corsRules CORSRULES] [--fileLockEnabled] [--replication REPLICATION] [--defaultServerSideEncryption {SSE-B2,none}] [--defaultServerSideEncryptionAlgorithm {AES256}] [--lifecycleRule LIFECYCLERULES | --lifecycleRules LIFECYCLERULES] bucketName {allPublic,allPrivate}
b2 create-key [-h] [--bucket BUCKET] [--namePrefix NAMEPREFIX] [--duration DURATION] [--allCapabilities] keyName [capabilities]
b2 delete-bucket [-h] bucketName
b2 delete-file-version [-h] [--bypassGovernance] [fileName] fileId
b2 delete-key [-h] applicationKeyId
b2 download-file-by-id [-h] [--threads THREADS] [--noProgress] [--sourceServerSideEncryption {SSE-C}] [--sourceServerSideEncryptionAlgorithm {AES256}] [--write-buffer-size BYTES] [--skip-hash-verification] [--max-download-streams-per-file MAX_DOWNLOAD_STREAMS_PER_FILE] fileId localFileName
b2 download-file-by-name [-h] [--threads THREADS] [--noProgress] [--sourceServerSideEncryption {SSE-C}] [--sourceServerSideEncryptionAlgorithm {AES256}] [--write-buffer-size BYTES] [--skip-hash-verification] [--max-download-streams-per-file MAX_DOWNLOAD_STREAMS_PER_FILE] bucketName b2FileName localFileName
b2 cat [-h] [--noProgress] [--sourceServerSideEncryption {SSE-C}] [--sourceServerSideEncryptionAlgorithm {AES256}] [--write-buffer-size BYTES] [--skip-hash-verification] b2uri
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
b2 ls [-h] [--long] [--json] [--replication] [--versions] [-r] [--withWildcard] bucketName [folderName]
b2 rm [-h] [--dryRun] [--queueSize QUEUESIZE] [--noProgress] [--failFast] [--threads THREADS] [--versions] [-r] [--withWildcard] bucketName [folderName]
b2 make-url [-h] fileId
b2 make-friendly-url [-h] bucketName fileName
b2 sync [-h] [--noProgress] [--dryRun] [--allowEmptySource] [--excludeAllSymlinks] [--syncThreads SYNCTHREADS] [--downloadThreads DOWNLOADTHREADS] [--uploadThreads UPLOADTHREADS] [--compareVersions {none,modTime,size}] [--compareThreshold MILLIS] [--excludeRegex REGEX] [--includeRegex REGEX] [--excludeDirRegex REGEX] [--excludeIfModifiedAfter TIMESTAMP] [--threads THREADS] [--destinationServerSideEncryption {SSE-B2,SSE-C}] [--destinationServerSideEncryptionAlgorithm {AES256}] [--sourceServerSideEncryption {SSE-C}] [--sourceServerSideEncryptionAlgorithm {AES256}] [--write-buffer-size BYTES] [--skip-hash-verification] [--max-download-streams-per-file MAX_DOWNLOAD_STREAMS_PER_FILE] [--incrementalMode] [--skipNewer | --replaceNewer] [--delete | --keepDays DAYS] source destination
b2 update-bucket [-h] [--bucketInfo BUCKETINFO] [--corsRules CORSRULES] [--defaultRetentionMode {compliance,governance,none}] [--defaultRetentionPeriod period] [--replication REPLICATION] [--fileLockEnabled] [--defaultServerSideEncryption {SSE-B2,none}] [--defaultServerSideEncryptionAlgorithm {AES256}] [--lifecycleRule LIFECYCLERULES | --lifecycleRules LIFECYCLERULES] bucketName [{allPublic,allPrivate}]
b2 upload-file [-h] [--contentType CONTENTTYPE] [--sha1 SHA1] [--cache-control CACHE_CONTROL] [--info INFO] [--custom-upload-timestamp CUSTOM_UPLOAD_TIMESTAMP] [--minPartSize MINPARTSIZE] [--threads THREADS] [--noProgress] [--destinationServerSideEncryption {SSE-B2,SSE-C}] [--destinationServerSideEncryptionAlgorithm {AES256}] [--legalHold {on,off}] [--fileRetentionMode {compliance,governance}] [--retainUntil TIMESTAMP] [--incrementalMode] bucketName localFilePath b2FileName
b2 upload-unbound-stream [-h] [--partSize PARTSIZE] [--unusedBufferTimeoutSeconds UNUSEDBUFFERTIMEOUTSECONDS] [--contentType CONTENTTYPE] [--sha1 SHA1] [--cache-control CACHE_CONTROL] [--info INFO] [--custom-upload-timestamp CUSTOM_UPLOAD_TIMESTAMP] [--minPartSize MINPARTSIZE] [--threads THREADS] [--noProgress] [--destinationServerSideEncryption {SSE-B2,SSE-C}] [--destinationServerSideEncryptionAlgorithm {AES256}] [--legalHold {on,off}] [--fileRetentionMode {compliance,governance}] [--retainUntil TIMESTAMP] bucketName localFilePath b2FileName
b2 update-file-legal-hold [-h] [fileName] fileId {on,off}
b2 update-file-retention [-h] [--retainUntil TIMESTAMP] [--bypassGovernance] [fileName] fileId {governance,compliance,none}
b2 replication-setup [-h] [--destination-profile DESTINATION_PROFILE] [--name NAME] [--priority PRIORITY] [--file-name-prefix PREFIX] [--include-existing-files] SOURCE_BUCKET_NAME DESTINATION_BUCKET_NAME
b2 replication-delete [-h] SOURCE_BUCKET_NAME REPLICATION_RULE_NAME
b2 replication-pause [-h] SOURCE_BUCKET_NAME REPLICATION_RULE_NAME
b2 replication-unpause [-h] SOURCE_BUCKET_NAME REPLICATION_RULE_NAME
b2 replication-status [-h] [--rule REPLICATION_RULE_NAME] [--destination-profile DESTINATION_PROFILE] [--dont-scan-destination] [--output-format {console,json,csv}] [--noProgress] [--columns COLUMN ONE,COLUMN TWO] SOURCE_BUCKET_NAME
b2 version [-h] [--short]
b2 license [-h]
b2 install-autocomplete [-h] [--shell {bash}]
```

The environment variable `B2_ACCOUNT_INFO` specifies the SQLite
file to use for caching authentication information.
The default file to use is: `~/.b2_account_info`.

To get more details on a specific command use `b2 <command> --help`.

When authorizing with application keys, this tool requires that the key
have the `listBuckets` capability so that it can take the bucket names
you provide on the command line and translate them into bucket IDs for the
B2 Storage service.  Each different command may required additional
capabilities. You can find the details for each command in the help for
that command.

### Parallelism and the `--threads` parameter

Users with high performance networks, file sets with very small files, or high network latency, will usually benefit from increased parallelism. Experiment with using the `--threads` parameter to increase performance.

Note that using many threads could in some cases be detrimental to the other users on your network.

### Docker image

#### Authorization

User can either authorize on each command (`list-buckets` is just a example here)

```bash
B2_APPLICATION_KEY=<key> B2_APPLICATION_KEY_ID=<key-id> docker run --rm -e B2_APPLICATION_KEY -e B2_APPLICATION_KEY_ID backblazeit/b2:latest list-buckets
```

or authorize once and keep the credentials persisted:

```bash
docker run --rm -it -v b2:/root backblazeit/b2:latest authorize-account
docker run --rm -v b2:/root backblazeit/b2:latest list-buckets  # remember to include `-v` - authorization details are there
```

#### Downloading and uploading

When uploading a single file, data can be passed to the container via a pipe:

```bash
cat source_file.txt | docker run --rm -v b2:/root backblazeit/b2:latest upload-unbound-stream bucket_name - target_file_name
```

or by mounting local files in the docker container:

```bash
docker run --rm -v b2:/root -v /home/user/path/to/data:/data backblazeit/b2:latest upload-file bucket_name /data/source_file.txt target_file_name
```

## Contrib

### Detailed logs

Verbose logs to stdout can be enabled with the `--verbose` flag.

A hidden flag `--debugLogs` can be used to enable logging to a `b2_cli.log` file (with log rotation at midnight) in current working directory. Please pay attention not to launch the tool from the directory that you are syncing, or the logs will get synced to the remote server (unless that is really what you want to achieve).

For advanced users, a hidden option `--logConfig <filename.ini>` can be used to enable logging in a user-defined format and verbosity. Check out the [example log configuration](contrib/debug_logs.ini).

In order to see the raw request headers you may `export B2_DEBUG_HTTP=1` before calling the CLI.

## Release History

Please refer to the [changelog](CHANGELOG.md).

## Developer Info

Please see our [contributing guidelines](CONTRIBUTING.md).
