# B2 Command Line Tool

[![CI](https://github.com/Backblaze/B2_Command_Line_Tool/actions/workflows/ci.yml/badge.svg)](https://github.com/Backblaze/B2_Command_Line_Tool/actions/workflows/ci.yml)&nbsp;[![License](https://img.shields.io/pypi/l/b2.svg?label=License)](https://pypi.python.org/pypi/b2)&nbsp;[![python versions](https://img.shields.io/pypi/pyversions/b2.svg?label=python%20versions)](https://pypi.python.org/pypi/b2)&nbsp;[![PyPI version](https://img.shields.io/pypi/v/b2.svg?label=PyPI%20version)](https://pypi.python.org/pypi/b2)&nbsp;[![Docs](https://readthedocs.org/projects/b2-command-line-tool/badge/?version=master)](https://b2-command-line-tool.readthedocs.io/en/master/?badge=master)

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

```
b2 account                Account management subcommands.
b2 bucket                 Bucket management subcommands.
b2 file                   File management subcommands.
b2 install-autocomplete   Install autocomplete for supported shells.
b2 key                    Application keys management subcommands.
b2 license                Print the license information for this tool.
b2 ls                     List files in a given folder.
b2 replication            Replication rule management subcommands.
b2 rm                     Remove a "folder" or a set of files matching a pattern.
b2 sync                   Copy multiple files from source to destination.
b2 version                Print the version number of this tool.
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

Thanks to [ApiVer methodology](#apiver-cli-versions-b2-vs-b2v3-b2v4-etc),
you should be perfectly fine using `b2:latest` version even in long-term support scripts,
but make sure to explicitly use `b2v4` command from the docker image as shown below.

#### Authorization

User can either authorize on each command (`bucket list` is just a example here)

```bash
B2_APPLICATION_KEY=<key> B2_APPLICATION_KEY_ID=<key-id> docker run --rm -e B2_APPLICATION_KEY -e B2_APPLICATION_KEY_ID backblazeit/b2:latest b2v4 bucket list
```

or authorize once and keep the credentials persisted:

```bash
docker run --rm -it -v b2:/root backblazeit/b2:latest b2v4 account authorize
docker run --rm -v b2:/root backblazeit/b2:latest b2v4 bucket list  # remember to include `-v` - authorization details are there
```

#### Downloading and uploading

When uploading a single file, data can be passed to the container via a pipe:

```bash
cat source_file.txt | docker run -i --rm -v b2:/root backblazeit/b2:latest b2v4 upload-unbound-stream bucket_name - target_file_name
```

or by mounting local files in the docker container:

```bash
docker run --rm -v b2:/root -v /home/user/path/to/data:/data backblazeit/b2:latest b2v4 file upload bucket_name /data/source_file.txt target_file_name
```

## ApiVer CLI versions (`b2` vs `b2v3`, `b2v4`, etc.)

Summary:

* in terminal, for best UX, use the latest apiver interface provided by `b2` command
* for long-term support, i.e. in scripts, use `b2v4` command

Explanation:

We use the `ApiVer` methodology so we can continue to evolve the `b2` command line tool,
while also providing all the bugfixes to the old interface versions.

If you use the `b2` command, you're working with the latest stable interface.
It provides all the bells and whistles, latest features, and the best performance.
While it's a great version to work with directly, but when writing a reliable, long-running script,
you want to ensure that your script won't break when we release a new version of the `b2` command.

In that case instead of using the `b2` command, you should use a version-bound interface e.g.: `b2v4`.
This command will always provide the same ApiVer 3 interface, regardless of the semantic version of the `b2` command.
Even if the `b2` command goes into the ApiVer `4`, `6` or even `10` with some major changes,
`b2v4` will still provide the same interface, same commands, and same parameters, with all the security and bug fixes.
Over time, it might get slower as we may need to emulate some older behaviors, but we'll ensure that it won't break.

You may find the next interface under `_b2v5`, but please note, as suggested by `_` prefix,
it is not yet stable and is not yet covered by guarantees listed above.

## Contrib

### Detailed logs

Verbose logs to stdout can be enabled with the `--verbose` flag.

A hidden flag `--debug-logs` can be used to enable logging to a `b2_cli.log` file (with log rotation at midnight) in current working directory. Please pay attention not to launch the tool from the directory that you are syncing, or the logs will get synced to the remote server (unless that is really what you want to achieve).

For advanced users, a hidden option `--log-config <filename.ini>` can be used to enable logging in a user-defined format and verbosity. Check out the [example log configuration](contrib/debug_logs.ini).

## Release History

Please refer to the [changelog](CHANGELOG.md).

## Developer Info

Please see our [contributing guidelines](CONTRIBUTING.md).
