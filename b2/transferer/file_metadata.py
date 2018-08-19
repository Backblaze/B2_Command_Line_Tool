######################################################################
#
# File: b2/transferer/file_metadata.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################


class FileMetadata(object):
    """ holds information about a file which is being downloaded """
    __slots__ = (
        'file_id',
        'file_name',
        'content_type',
        'content_length',
        'content_sha1',
        'file_info',
    )

    def __init__(
        self,
        file_id,
        file_name,
        content_type,
        content_length,
        content_sha1,
        file_info,
    ):
        self.file_id = file_id
        self.file_name = file_name
        self.content_type = content_type
        self.content_length = content_length
        self.content_sha1 = content_sha1
        self.file_info = file_info

    @classmethod
    def from_response(cls, response):
        info = response.headers
        return cls(
            file_id=info['x-bz-file-id'],
            file_name=info['x-bz-file-name'],
            content_type=info['content-type'],
            content_length=int(info['content-length']),
            content_sha1=info['x-bz-content-sha1'],
            file_info=dict((k[10:], info[k]) for k in info if k.startswith('x-bz-info-')),
        )

    def as_info_dict(self):
        return {
            'fileId': self.file_id,
            'fileName': self.file_name,
            'contentType': self.content_type,
            'contentLength': self.content_length,
            'contentSha1': self.content_sha1,
            'fileInfo': self.file_info,
        }
