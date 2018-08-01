######################################################################
#
# File: b2/part.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################


class PartFactory(object):
    @classmethod
    def from_list_parts_dict(cls, part_dict):
        return Part(
            part_dict['fileId'], part_dict['partNumber'], part_dict['contentLength'],
            part_dict['contentSha1']
        )


class Part(object):
    def __init__(self, file_id, part_number, content_length, content_sha1):
        self.file_id = file_id
        self.part_number = part_number
        self.content_length = content_length
        self.content_sha1 = content_sha1

    def __repr__(self):
        return '<%s %s %s %s %s>' % (
            self.__class__.__name__, self.file_id, self.part_number, self.content_length,
            self.content_sha1
        )

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
