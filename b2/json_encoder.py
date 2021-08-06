######################################################################
#
# File: b2/json_encoder.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import json

from b2sdk.v2 import FileVersion, FileIdAndName, Bucket


class B2CliJsonEncoder(json.JSONEncoder):
    """
    Makes it possible to serialize b2sdk objects
    (specifically bucket['options'] set and FileVersionInfo/FileIdAndName) to json.

    >>> json.dumps(set([1,2,3,'a','b','c']), cls=json_encoder.B2CliJsonEncoder)
    '[1, 2, 3, "c", "b", "a"]'
    >>>
    """

    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, (FileVersion, FileIdAndName, Bucket)):
            return obj.as_dict()
        return super().default(obj)
