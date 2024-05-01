######################################################################
#
# File: b2/_internal/b2v4/registry.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# ruff: noqa: F405
from b2._internal.console_tool import *  # noqa

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
