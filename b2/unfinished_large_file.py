######################################################################
#
# File: b2/unfinished_large_file.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################


class UnfinishedLargeFile(object):
    def __init__(self, file_dict):
        """
        Initializes from one file returned by b2_start_large_file,
        or b2_list_unfinished_large_files.
        """
        self.file_id = file_dict['fileId']
        self.file_name = file_dict['fileName']
        self.account_id = file_dict['accountId']
        self.bucket_id = file_dict['bucketId']
        self.content_type = file_dict['contentType']
        self.file_info = file_dict['fileInfo']

    def __repr__(self):
        return '<%s %s %s>' % (self.__class__.__name__, self.bucket_id, self.file_name)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
