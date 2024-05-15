######################################################################
#
# File: b2/_internal/b2v3/registry.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# ruff: noqa: F405
from b2._internal.b2v4.registry import *  # noqa
from b2._internal._cli.b2api import _get_b2api_for_profile
from b2._internal.arg_parser import enable_camel_case_arguments
from .rm import Rm, B2URIMustPointToFolderMixin
from .sync import Sync

enable_camel_case_arguments()


class ConsoleTool(ConsoleTool):
    # same as original console tool, but does not use InMemoryAccountInfo and InMemoryCache
    # when auth env vars are used

    @classmethod
    def _initialize_b2_api(cls, args: argparse.Namespace, kwargs: dict) -> B2Api:
        return _get_b2api_for_profile(profile=args.profile, **kwargs)


def main() -> None:
    # this is a copy of v4 `main()` but with custom console tool class

    ct = ConsoleTool(stdout=sys.stdout, stderr=sys.stderr)
    exit_status = ct.run_command(sys.argv)
    logger.info('\\\\ %s %s %s //', SEPARATOR, ('exit=%s' % exit_status).center(8), SEPARATOR)

    # I haven't tracked down the root cause yet, but in Python 2.7, the futures
    # packages is hanging on exit sometimes, waiting for a thread to finish.
    # This happens when using sync to upload files.
    sys.stdout.flush()
    sys.stderr.flush()

    logging.shutdown()

    os._exit(exit_status)


class Ls(B2URIMustPointToFolderMixin, B2URIBucketNFolderNameArgMixin, BaseLs):
    """
    {BaseLs}

    Examples

    .. note::

        Note the use of quotes, to ensure that special
        characters are not expanded by the shell.


    List csv and tsv files (in any directory, in the whole bucket):

    .. code-block::

        {NAME} ls --recursive --withWildcard bucketName "*.[ct]sv"


    List all info.txt files from directories `b?`, where `?` is any character:

    .. code-block::

        {NAME} ls --recursive --withWildcard bucketName "b?/info.txt"


    List all pdf files from directories b0 to b9 (including sub-directories):

    .. code-block::

        {NAME} ls --recursive --withWildcard bucketName "b[0-9]/*.pdf"


    List all buckets:

    .. code-block::

        {NAME} ls

    Requires capability:

    - **listFiles**
    - **listBuckets** (if bucket name is not provided)
    """
    ALLOW_ALL_BUCKETS = True


class HyphenFilenameMixin:
    def get_input_stream(self, filename):
        if filename == '-' and os.path.exists('-'):
            self._print_stderr(
                "WARNING: Filename `-` won't be supported in the future and will always be treated as stdin alias."
            )
            return '-'
        return super().get_input_stream(filename)


class UploadUnboundStream(HyphenFilenameMixin, UploadUnboundStream):
    __doc__ = UploadUnboundStream.__doc__


class UploadFile(HyphenFilenameMixin, UploadFile):
    __doc__ = UploadFile.__doc__


B2.register_subcommand(AuthorizeAccount)
B2.register_subcommand(CancelAllUnfinishedLargeFiles)
B2.register_subcommand(CancelLargeFile)
B2.register_subcommand(ClearAccount)
B2.register_subcommand(CopyFileById)
B2.register_subcommand(CreateBucket)
B2.register_subcommand(CreateKey)
B2.register_subcommand(DeleteBucket)
B2.register_subcommand(DeleteFileVersion)
B2.register_subcommand(DeleteKey)
B2.register_subcommand(DownloadFile)
B2.register_subcommand(DownloadFileById)
B2.register_subcommand(DownloadFileByName)
B2.register_subcommand(Cat)
B2.register_subcommand(GetAccountInfo)
B2.register_subcommand(GetBucket)
B2.register_subcommand(FileInfo2)
B2.register_subcommand(GetFileInfo)
B2.register_subcommand(GetDownloadAuth)
B2.register_subcommand(GetDownloadUrlWithAuth)
B2.register_subcommand(HideFile)
B2.register_subcommand(ListBuckets)
B2.register_subcommand(ListKeys)
B2.register_subcommand(ListParts)
B2.register_subcommand(ListUnfinishedLargeFiles)
B2.register_subcommand(Ls)
B2.register_subcommand(Rm)
B2.register_subcommand(GetUrl)
B2.register_subcommand(MakeUrl)
B2.register_subcommand(MakeFriendlyUrl)
B2.register_subcommand(Sync)
B2.register_subcommand(UpdateBucket)
B2.register_subcommand(UploadFile)
B2.register_subcommand(UploadUnboundStream)
B2.register_subcommand(UpdateFileLegalHold)
B2.register_subcommand(UpdateFileRetention)
B2.register_subcommand(ReplicationSetup)
B2.register_subcommand(ReplicationDelete)
B2.register_subcommand(ReplicationPause)
B2.register_subcommand(ReplicationUnpause)
B2.register_subcommand(ReplicationStatus)
B2.register_subcommand(Version)
B2.register_subcommand(License)
B2.register_subcommand(InstallAutocomplete)
B2.register_subcommand(NotificationRules)
B2.register_subcommand(Key)
B2.register_subcommand(Replication)
B2.register_subcommand(Account)
B2.register_subcommand(BucketCmd)
B2.register_subcommand(File)
