# B2 Command Line Tool

[![Continuous Integration](https://github.com/Backblaze/B2_Command_Line_Tool/actions/workflows/ci.yml/badge.svg)](https://github.com/Backblaze/B2_Command_Line_Tool/actions/workflows/ci.yml)&nbsp;[![License](https://img.shields.io/pypi/l/b2.svg?label=License)](https://pypi.python.org/pypi/b2)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/b2.svg?label=python%20versions)](https://pypi.python.org/pypi/b2)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/b2.svg?label=PyPI%20version)](https://pypi.python.org/pypi/b2)&nbsp;[![Docs](https://readthedocs.org/projects/b2-command-line-tool/badge/?version=master)](https://b2-command-line-tool.readthedocs.io/en/master/?badge=master)

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

Not recommended, unless you want to check if a current pre-release code solves a bug affecting you.

```bash
pip install git+https://github.com/Backblaze/B2_Command_Line_Tool.git
```

If you wish to contribute to or otherwise modify source code, please see our [contributing guidelines](CONTRIBUTING.md).

## Usage

```bash
b2 authorize-account [-h]  [applicationKeyId] [applicationKey]
b2 cancel-all-unfinished-large-files [-h] bucketName
b2 cancel-large-file [-h] fileId
b2 clear-account [-h]
b2 copy-file-by-id [-h] [--fetch-metadata] [--content-type CONTENTTYPE] [--range RANGE] [--info INFO | --no-info] [--destination-server-side-encryption {SSE-B2,SSE-C}] [--destination-server-side-encryption-algorithm {AES256}] [--source-server-side-encryption {SSE-C}] [--source-server-side-encryption-algorithm {AES256}] [--file-retention-mode {compliance,governance}] [--retain-until TIMESTAMP] [--legal-hold {on,off}] sourceFileId destinationBucketName b2FileName
b2 create-bucket [-h] [--bucket-info BUCKETINFO] [--cors-rules CORSRULES] [--file-lock-enabled] [--replication REPLICATION] [--default-server-side-encryption {SSE-B2,none}] [--default-server-side-encryption-algorithm {AES256}] [--lifecycle-rule LIFECYCLERULES | --lifecycle-rules LIFECYCLERULES] bucketName {allPublic,allPrivate}
b2 create-key [-h] [--bucket BUCKET] [--name-prefix NAMEPREFIX] [--duration DURATION] [--all-capabilities] keyName [capabilities]
b2 delete-bucket [-h] bucketName
b2 delete-file-version [-h] [--bypass-governance] [fileName] fileId
b2 delete-key [-h] applicationKeyId
b2 download-file [-h] [--threads THREADS] [--max-download-streams-per-file MAX_DOWNLOAD_STREAMS_PER_FILE] [--no-progress] [--source-server-side-encryption {SSE-C}] [--source-server-side-encryption-algorithm {AES256}] [--write-buffer-size BYTES] [--skip-hash-verification] B2_URI localFileName
b2 cat [-h] [--no-progress] [--source-server-side-encryption {SSE-C}] [--source-server-side-encryption-algorithm {AES256}] [--write-buffer-size BYTES] [--skip-hash-verification] B2_URI
b2 get-account-info [-h]
b2 get-bucket [-h] [--show-size] bucketName
b2 file-info [-h] B2_URI
b2 get-download-auth [-h] [--prefix PREFIX] [--duration DURATION] bucketName
b2 get-download-url-with-auth [-h] [--duration DURATION] bucketName fileName
b2 hide-file [-h] bucketName fileName
b2 list-buckets [-h] [--json]
b2 list-keys [-h] [--long]
b2 list-parts [-h] largeFileId
b2 list-unfinished-large-files [-h] bucketName
b2 ls [-h] [--long] [--json] [--replication] [--versions] [-r] [--with-wildcard] bucketName [folderName]
b2 rm [-h] [--dry-run] [--queue-size QUEUESIZE] [--no-progress] [--fail-fast] [--threads THREADS] [--versions] [-r] [--with-wildcard] bucketName [folderName]
b2 get-url [-h] B2_URI
b2 sync [-h] [--no-progress] [--dry-run] [--allow-empty-source] [--exclude-all-symlinks] [--sync-threads SYNCTHREADS] [--download-threads DOWNLOADTHREADS] [--upload-threads UPLOADTHREADS] [--compare-versions {none,modTime,size}] [--compare-threshold MILLIS] [--exclude-regex REGEX] [--include-regex REGEX] [--exclude-dir-regex REGEX] [--exclude-if-modified-after TIMESTAMP] [--threads THREADS] [--destination-server-side-encryption {SSE-B2,SSE-C}] [--destination-server-side-encryption-algorithm {AES256}] [--source-server-side-encryption {SSE-C}] [--source-server-side-encryption-algorithm {AES256}] [--write-buffer-size BYTES] [--skip-hash-verification] [--max-download-streams-per-file MAX_DOWNLOAD_STREAMS_PER_FILE] [--incremental-mode] [--skip-newer | --replace-newer] [--delete | --keep-days DAYS] source destination
b2 update-bucket [-h] [--bucket-info BUCKETINFO] [--cors-rules CORSRULES] [--default-retention-mode {compliance,governance,none}] [--default-retention-period period] [--replication REPLICATION] [--file-lock-enabled] [--default-server-side-encryption {SSE-B2,none}] [--default-server-side-encryption-algorithm {AES256}] [--lifecycle-rule LIFECYCLERULES | --lifecycle-rules LIFECYCLERULES] bucketName [{allPublic,allPrivate}]
b2 upload-file [-h] [--content-type CONTENTTYPE] [--sha1 SHA1] [--cache-control CACHE_CONTROL] [--info INFO] [--custom-upload-timestamp CUSTOM_UPLOAD_TIMESTAMP] [--min-part-size MINPARTSIZE] [--threads THREADS] [--no-progress] [--destination-server-side-encryption {SSE-B2,SSE-C}] [--destination-server-side-encryption-algorithm {AES256}] [--legal-hold {on,off}] [--file-retention-mode {compliance,governance}] [--retain-until TIMESTAMP] [--incremental-mode] bucketName localFilePath b2FileName
b2 upload-unbound-stream [-h] [--part-size PARTSIZE] [--unused-buffer-timeout-seconds UNUSEDBUFFERTIMEOUTSECONDS] [--content-type CONTENTTYPE] [--sha1 SHA1] [--cache-control CACHE_CONTROL] [--info INFO] [--custom-upload-timestamp CUSTOM_UPLOAD_TIMESTAMP] [--min-part-size MINPARTSIZE] [--threads THREADS] [--no-progress] [--destination-server-side-encryption {SSE-B2,SSE-C}] [--destination-server-side-encryption-algorithm {AES256}] [--legal-hold {on,off}] [--file-retention-mode {compliance,governance}] [--retain-until TIMESTAMP] bucketName localFilePath b2FileName
b2 update-file-legal-hold [-h] [fileName] fileId {on,off}
b2 update-file-retention [-h] [--retain-until TIMESTAMP] [--bypass-governance] [fileName] fileId {governance,compliance,none}
b2 replication-setup [-h] [--destination-profile DESTINATION_PROFILE] [--name NAME] [--priority PRIORITY] [--file-name-prefix PREFIX] [--include-existing-files] SOURCE_BUCKET_NAME DESTINATION_BUCKET_NAME
b2 replication-delete [-h] SOURCE_BUCKET_NAME REPLICATION_RULE_NAME
b2 replication-pause [-h] SOURCE_BUCKET_NAME REPLICATION_RULE_NAME
b2 replication-unpause [-h] SOURCE_BUCKET_NAME REPLICATION_RULE_NAME
b2 replication-status [-h] [--rule REPLICATION_RULE_NAME] [--destination-profile DESTINATION_PROFILE] [--dont-scan-destination] [--output-format {console,json,csv}] [--no-progress] [--columns COLUMN ONE,COLUMN TWO] SOURCE_BUCKET_NAME
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
cat source_file.txt | docker run -i --rm -v b2:/root backblazeit/b2:latest upload-unbound-stream bucket_name - target_file_name
```

or by mounting local files in the docker container:

```bash
docker run --rm -v b2:/root -v /home/user/path/to/data:/data backblazeit/b2:latest upload-file bucket_name /data/source_file.txt target_file_name
```

## Versions

When you start working with `b2`, you might notice that more than one script is available to you.
This is by design - we use the `ApiVer` methodology to provide all the commands to all the versions
while also providing all the bugfixes to all the old versions.

If you use the `b2` command, you're working with the latest stable version.
It provides all the bells and whistles, latest features, and the best performance.
While it's a great version to work with, if you're willing to write a reliable, long-running script,
you might find out that after some time it will break.
New commands will appear, older will deprecate and be removed, parameters will change.
Backblaze service evolves and the `b2` CLI evolves with it.

However, now you have a way around this problem.
Instead of using the `b2` command, you can use a version-bound interface e.g.: `b2v3`.
This command will always provide the same interface that the `ApiVer` version `3` provided.
Even if the `b2` command goes into the `ApiVer` version `4`, `6` or even `10` with some major changes,
`b2v3` will still provide the same interface, same commands, and same parameters.
Over time, it might get slower as we may need to emulate some older behaviors, but we'll ensure that it won't break.

## Contrib

### Detailed logs

Verbose logs to stdout can be enabled with the `--verbose` flag.

A hidden flag `--debug-logs` can be used to enable logging to a `b2_cli.log` file (with log rotation at midnight) in current working directory. Please pay attention not to launch the tool from the directory that you are syncing, or the logs will get synced to the remote server (unless that is really what you want to achieve).

For advanced users, a hidden option `--log-config <filename.ini>` can be used to enable logging in a user-defined format and verbosity. Check out the [example log configuration](contrib/debug_logs.ini).

## Release History

Please refer to the [changelog](CHANGELOG.md).

## Developer Info

Please see our [contributing guidelines](CONTRIBUTING.md).
