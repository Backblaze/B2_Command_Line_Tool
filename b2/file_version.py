######################################################################
#
# File: b2/file_version.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import datetime


class FileVersionInfo(object):
    LS_ENTRY_TEMPLATE = '%83s  %6s  %10s  %8s  %9d  %s'  # order is file_id, action, date, time, size, name

    def __init__(
        self, id_, file_name, size, content_type, content_sha1, file_info, upload_timestamp, action
    ):
        self.id_ = id_
        self.file_name = file_name
        self.size = size  # can be None (unknown)
        self.content_type = content_type
        self.content_sha1 = content_sha1
        self.file_info = file_info or {}
        self.upload_timestamp = upload_timestamp  # can be None (unknown)
        self.action = action  # "upload" or "hide" or "delete"

    def as_dict(self):
        result = {
            'fileId': self.id_,
            'fileName': self.file_name,
        }

        if self.size is not None:
            result['size'] = self.size
        if self.upload_timestamp is not None:
            result['uploadTimestamp'] = self.upload_timestamp
        if self.action is not None:
            result['action'] = self.action
        return result

    def format_ls_entry(self):
        dt = datetime.datetime.utcfromtimestamp(self.upload_timestamp / 1000)
        date_str = dt.strftime('%Y-%m-%d')
        time_str = dt.strftime('%H:%M:%S')
        size = self.size or 0  # required if self.action == 'hide'
        return self.LS_ENTRY_TEMPLATE % (
            self.id_,
            self.action,
            date_str,
            time_str,
            size,
            self.file_name,
        )

    @classmethod
    def format_folder_ls_entry(cls, name):
        return cls.LS_ENTRY_TEMPLATE % ('-', '-', '-', '-', 0, name)


class FileVersionInfoFactory(object):
    @classmethod
    def from_api_response(cls, file_info_dict, force_action=None):
        """
            turns this:
            {
              "action": "hide",
              "fileId": "4_zBucketName_f103b7ca31313c69c_d20151230_m030117_c001_v0001015_t0000",
              "fileName": "randomdata",
              "size": 0,
              "uploadTimestamp": 1451444477000
            }
            or this:
            {
              "accountId": "4aa9865d6f00",
              "bucketId": "547a2a395826655d561f0010",
              "contentLength": 1350,
              "contentSha1": "753ca1c2d0f3e8748320b38f5da057767029a036",
              "contentType": "application/octet-stream",
              "fileId": "4_z547a2a395826655d561f0010_f106d4ca95f8b5b78_d20160104_m003906_c001_v0001013_t0005",
              "fileInfo": {},
              "fileName": "randomdata"
            }
            into a FileVersionInfo object
        """
        assert file_info_dict.get('action') is None or force_action is None, \
            'action was provided by both info_dict and function argument'
        action = file_info_dict.get('action') or force_action
        file_name = file_info_dict['fileName']
        id_ = file_info_dict['fileId']
        if 'size' in file_info_dict:
            size = file_info_dict['size']
        elif 'contentLength' in file_info_dict:
            size = file_info_dict['contentLength']
        else:
            raise ValueError('no size or contentLength')
        upload_timestamp = file_info_dict.get('uploadTimestamp')
        content_type = file_info_dict.get('contentType')
        content_sha1 = file_info_dict.get('contentSha1')
        file_info = file_info_dict.get('fileInfo')

        return FileVersionInfo(
            id_, file_name, size, content_type, content_sha1, file_info, upload_timestamp, action
        )

    @classmethod
    def from_cancel_large_file_response(cls, response):
        return FileVersionInfo(
            response['fileId'],
            response['fileName'],
            0,  # size
            'unknown',
            'none',
            {},
            0,  # upload timestamp
            'cancel'
        )


class FileIdAndName(object):
    def __init__(self, file_id, file_name):
        self.file_id = file_id
        self.file_name = file_name

    def as_dict(self):
        return {'action': 'delete', 'fileId': self.file_id, 'fileName': self.file_name}
