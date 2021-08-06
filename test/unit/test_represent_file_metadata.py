######################################################################
#
# File: test/unit/test_represent_file_metadata.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from io import StringIO
from b2sdk.v2 import B2Api
from b2sdk.v2 import B2HttpApiConfig
from b2sdk.v2 import StubAccountInfo
from b2.console_tool import ConsoleTool
from b2sdk.v2 import FileRetentionSetting, RetentionMode, LegalHold
from b2sdk.v2 import EncryptionSetting, EncryptionMode, EncryptionKey, EncryptionAlgorithm, SSE_B2_AES
from b2sdk.v2 import RawSimulator
from .test_base import TestBase


class TestReprentFileMetadata(TestBase):
    def setUp(self):
        self.master_b2_api = B2Api(
            StubAccountInfo(), None, api_config=B2HttpApiConfig(_raw_api_class=RawSimulator)
        )
        self.raw_api = self.master_b2_api.session.raw_api
        (self.master_account_id, self.master_key) = self.raw_api.create_account()
        self.master_b2_api.authorize_account('production', self.master_account_id, self.master_key)
        self.lock_enabled_bucket = self.master_b2_api.create_bucket(
            'lock-enabled-bucket', 'allPrivate', is_file_lock_enabled=True
        )
        self.lock_disabled_bucket = self.master_b2_api.create_bucket(
            'lock-disabled-bucket', 'allPrivate', is_file_lock_enabled=False
        )
        new_key = self.master_b2_api.create_key(
            [
                'listKeys',
                'listBuckets',
                'listFiles',
                'readFiles',
            ], 'restricted'
        )
        self.restricted_key_id, self.restricted_key = new_key.id_, new_key.application_key

        self.restricted_b2_api = B2Api(StubAccountInfo(), None)
        self.restricted_b2_api.session.raw_api = self.raw_api
        self.restricted_b2_api.authorize_account(
            'production', self.restricted_key_id, self.restricted_key
        )

        self.stdout = StringIO()
        self.stderr = StringIO()
        self.console_tool = ConsoleTool(self.master_b2_api, self.stdout, self.stderr)

    def assertRetentionRepr(self, file_id: str, api: B2Api, expected_repr: str):
        file_version = api.get_file_info(file_id)
        assert self.console_tool._represent_retention(file_version.file_retention) == expected_repr

    def assertLegalHoldRepr(self, file_id: str, api: B2Api, expected_repr: str):
        file_version = api.get_file_info(file_id)
        assert self.console_tool._represent_legal_hold(file_version.legal_hold) == expected_repr

    def assertEncryptionRepr(self, file_id: str, expected_repr: str):
        file_version = self.master_b2_api.get_file_info(file_id)
        assert self.console_tool._represent_encryption(
            file_version.server_side_encryption
        ) == expected_repr

    def test_file_retention(self):
        file = self.lock_disabled_bucket.upload_bytes(b'insignificant', 'file')
        self.assertRetentionRepr(file.id_, self.master_b2_api, 'none')
        self.assertRetentionRepr(file.id_, self.restricted_b2_api, '<unauthorized to read>')

        file = self.lock_enabled_bucket.upload_bytes(b'insignificant', 'file')
        self.assertRetentionRepr(file.id_, self.master_b2_api, 'none')
        self.assertRetentionRepr(file.id_, self.restricted_b2_api, '<unauthorized to read>')

        self.master_b2_api.update_file_retention(
            file.id_, file.file_name, FileRetentionSetting(RetentionMode.GOVERNANCE, 1500)
        )
        self.assertRetentionRepr(
            file.id_, self.master_b2_api,
            'mode=governance, retainUntil=1970-01-01 00:00:01.500000+00:00'
        )
        self.assertRetentionRepr(file.id_, self.restricted_b2_api, '<unauthorized to read>')

        self.master_b2_api.update_file_retention(
            file.id_, file.file_name, FileRetentionSetting(RetentionMode.COMPLIANCE, 2000)
        )

        self.assertRetentionRepr(
            file.id_, self.master_b2_api, 'mode=compliance, retainUntil=1970-01-01 00:00:02+00:00'
        )
        self.assertRetentionRepr(file.id_, self.restricted_b2_api, '<unauthorized to read>')

    def test_legal_hold(self):
        file = self.lock_disabled_bucket.upload_bytes(b'insignificant', 'file')
        self.assertLegalHoldRepr(file.id_, self.master_b2_api, '<unset>')
        self.assertLegalHoldRepr(file.id_, self.restricted_b2_api, '<unauthorized to read>')

        file = self.lock_enabled_bucket.upload_bytes(b'insignificant', 'file')
        self.assertLegalHoldRepr(file.id_, self.master_b2_api, '<unset>')
        self.assertLegalHoldRepr(file.id_, self.restricted_b2_api, '<unauthorized to read>')

        self.master_b2_api.update_file_legal_hold(file.id_, file.file_name, LegalHold.ON)
        self.assertLegalHoldRepr(file.id_, self.master_b2_api, 'on')
        self.assertLegalHoldRepr(file.id_, self.restricted_b2_api, '<unauthorized to read>')

        self.master_b2_api.update_file_legal_hold(file.id_, file.file_name, LegalHold.OFF)
        self.assertLegalHoldRepr(file.id_, self.master_b2_api, 'off')
        self.assertLegalHoldRepr(file.id_, self.restricted_b2_api, '<unauthorized to read>')

    def test_encryption(self):
        file = self.lock_enabled_bucket.upload_bytes(b'insignificant', 'file')
        self.assertEncryptionRepr(file.id_, 'none')

        file = self.lock_enabled_bucket.upload_bytes(
            b'insignificant', 'file', encryption=SSE_B2_AES
        )
        self.assertEncryptionRepr(file.id_, 'mode=SSE-B2, algorithm=AES256')

        file = self.lock_enabled_bucket.upload_bytes(
            b'insignificant',
            'file',
            encryption=EncryptionSetting(
                EncryptionMode.SSE_C,
                algorithm=EncryptionAlgorithm.AES256,
                key=EncryptionKey(b'', key_id=None),
            )
        )
        self.assertEncryptionRepr(file.id_, 'mode=SSE-C, algorithm=AES256')

        file = self.lock_enabled_bucket.upload_bytes(
            b'insignificant',
            'file',
            encryption=EncryptionSetting(
                EncryptionMode.SSE_C,
                algorithm=EncryptionAlgorithm.AES256,
                key=EncryptionKey(b'', key_id='some_id'),
            )
        )
        self.assertEncryptionRepr(file.id_, 'mode=SSE-C, algorithm=AES256, key_id=some_id')
