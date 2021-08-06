######################################################################
#
# File: test/unit/test_copy.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from unittest import mock

from b2.console_tool import CopyFileById
from b2sdk.v2 import EncryptionSetting, EncryptionMode, EncryptionKey, EncryptionAlgorithm, SSE_B2_AES, UNKNOWN_KEY_ID
from .test_base import TestBase


class TestCopy(TestBase):
    def test_determine_source_metadata(self):
        mock_api = mock.MagicMock()
        mock_console_tool = mock.MagicMock()
        mock_console_tool.api = mock_api
        copy_file_command = CopyFileById(mock_console_tool)

        result = copy_file_command._determine_source_metadata(
            'id',
            destination_encryption=None,
            source_encryption=None,
            target_file_info=None,
            target_content_type=None,
            fetch_if_necessary=True,
        )
        assert result == (None, None)
        assert len(mock_api.method_calls) == 0

        result = copy_file_command._determine_source_metadata(
            'id',
            destination_encryption=SSE_B2_AES,
            source_encryption=SSE_B2_AES,
            target_file_info={},
            target_content_type='',
            fetch_if_necessary=True,
        )
        assert result == (None, None)
        assert len(mock_api.method_calls) == 0

        result = copy_file_command._determine_source_metadata(
            'id',
            destination_encryption=SSE_B2_AES,
            source_encryption=SSE_B2_AES,
            target_file_info={},
            target_content_type='',
            fetch_if_necessary=True,
        )
        assert result == (None, None)
        assert len(mock_api.method_calls) == 0

        source_sse_c = EncryptionSetting(
            EncryptionMode.SSE_C, EncryptionAlgorithm.AES256,
            EncryptionKey(b'some_key', UNKNOWN_KEY_ID)
        )
        destination_sse_c = EncryptionSetting(
            EncryptionMode.SSE_C, EncryptionAlgorithm.AES256,
            EncryptionKey(b'some_other_key', 'key_id')
        )

        result = copy_file_command._determine_source_metadata(
            'id',
            destination_encryption=destination_sse_c,
            source_encryption=source_sse_c,
            target_file_info={},
            target_content_type='',
            fetch_if_necessary=True,
        )
        assert result == (None, None)
        assert len(mock_api.method_calls) == 0

        with self.assertRaises(
            ValueError, 'Attempting to copy file with metadata while either source or '
            'destination uses SSE-C. Use --fetchMetadata to fetch source '
            'file metadata before copying.'
        ):
            copy_file_command._determine_source_metadata(
                'id',
                destination_encryption=destination_sse_c,
                source_encryption=source_sse_c,
                target_file_info=None,
                target_content_type=None,
                fetch_if_necessary=False,
            )
        assert len(mock_api.method_calls) == 0

        result = copy_file_command._determine_source_metadata(
            'id',
            destination_encryption=destination_sse_c,
            source_encryption=source_sse_c,
            target_file_info=None,
            target_content_type=None,
            fetch_if_necessary=True,
        )
        assert result != (None, None)
        assert len(mock_api.method_calls)
