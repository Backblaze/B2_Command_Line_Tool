######################################################################
#
# File: b2/sync/policy_manager.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from .policy import DownAndDeletePolicy, DownAndKeepDaysPolicy, DownPolicy
from .policy import UpAndDeletePolicy, UpAndKeepDaysPolicy, UpPolicy


class SyncPolicyManager(object):
    def __init__(self):
        self.policies = {}  # dict<,>

    def get_policy(
        self, sync_type, source_file, source_folder, dest_file, dest_folder, now_millis, args
    ):
        policy_class = self.get_policy_class(sync_type, args)
        return policy_class(source_file, source_folder, dest_file, dest_folder, now_millis, args)

    def get_policy_class(self, sync_type, args):
        if sync_type == 'local-to-b2':
            if args.delete:
                return UpAndDeletePolicy
            elif args.keepDays:
                return UpAndKeepDaysPolicy
            else:
                return UpPolicy
        elif sync_type == 'b2-to-local':
            if args.delete:
                return DownAndDeletePolicy
            elif args.keepDays:
                return DownAndKeepDaysPolicy
            else:
                return DownPolicy
        assert False, 'invalid sync type: %s, args: %s' % (sync_type, str(args))


POLICY_MANAGER = SyncPolicyManager()
