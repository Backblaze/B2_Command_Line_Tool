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


class SetToListEncoder(json.JSONEncoder):
    """
    Makes it possible to serialize b2sdk bucket objects
    (specifically bucket['options'] set) to json.

    >>> json.dumps(set([1,2,3,'a','b','c']), cls=json_encoder.SetToListEncoder)
    '[1, 2, 3, "c", "b", "a"]'
    >>>
    """

    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return super(SetToListEncoder, self).default(self, obj)
