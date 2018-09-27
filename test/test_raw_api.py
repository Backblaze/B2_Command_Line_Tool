######################################################################
#
# File: b2/test_raw_api.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import print_function

from six import unichr

from b2.raw_api import B2RawApi
from b2.b2http import B2Http
from b2.exception import UnusableFileName
from .test_base import TestBase

# Unicode characters for testing filenames.  (0x0394 is a letter Delta.)
TWO_BYTE_UNICHR = unichr(0x0394)
CHAR_UNDER_32 = unichr(31)
DEL_CHAR = unichr(127)


class TestRawAPIFilenames(TestBase):
    """Test that the filename checker passes conforming names and rejects those that don't."""

    def setUp(self):
        self.raw_api = B2RawApi(B2Http())

    def _should_be_ok(self, filename):
        """Call with test filenames that follow the filename rules.

        :param filename: unicode (or str) that follows the rules
        """
        print(u"Filename \"{0}\" should be OK".format(filename))
        self.assertTrue(self.raw_api.check_b2_filename(filename) is None)

    def _should_raise(self, filename, exception_message):
        """Call with filenames that don't follow the rules (so the rule checker should raise).

        :param filename: unicode (or str) that doesn't follow the rules
        :param exception_message: regexp that matches the exception's detailed message
        """
        print(
            u"Filename \"{0}\" should raise UnusableFileName(\".*{1}.*\").".format(
                filename, exception_message
            )
        )
        with self.assertRaisesRegexp(UnusableFileName, exception_message):
            self.raw_api.check_b2_filename(filename)

    def test_b2_filename_checker(self):
        """Test a conforming and non-conforming filename for each rule.

        From the B2 docs (https://www.backblaze.com/b2/docs/files.html):
        - Names can be pretty much any UTF-8 string up to 1024 bytes long.
        - No character codes below 32 are allowed.
        - Backslashes are not allowed.
        - DEL characters (127) are not allowed.
        - File names cannot start with "/", end with "/", or contain "//".
        - Maximum of 250 bytes of UTF-8 in each segment (part between slashes) of a file name.
        """
        print("test b2 filename rules")

        # Examples from doc:
        self._should_be_ok('Kitten Videos')
        self._should_be_ok(u'\u81ea\u7531.txt')

        # Check length
        # 1024 bytes is ok if the segments are at most 250 chars.
        s_1024 = 4 * (250 * 'x' + '/') + 20 * 'y'
        self._should_be_ok(s_1024)
        # 1025 is too long.
        self._should_raise(s_1024 + u'x', "too long")
        # 1024 bytes with two byte characters should also work.
        s_1024_two_byte = 4 * (125 * TWO_BYTE_UNICHR + u'/') + 20 * u'y'
        self._should_be_ok(s_1024_two_byte)
        # But 1025 bytes is too long.
        self._should_raise(s_1024_two_byte + u'x', "too long")

        # Names with unicode values < 32, and DEL aren't allowed.
        self._should_raise(u'hey' + CHAR_UNDER_32, "contains code.*less than 32")
        # Unicode in the filename shouldn't break the exception message.
        self._should_raise(TWO_BYTE_UNICHR + CHAR_UNDER_32, "contains code.*less than 32")
        self._should_raise(DEL_CHAR, "DEL.*not allowed")

        # Names can't start or end with '/' or contain '//'
        self._should_raise(u'/hey', "not start.*/")
        self._should_raise(u'hey/', "not .*end.*/")
        self._should_raise(u'not//allowed', "contain.*//")

        # Reject segments longer than 250 bytes
        self._should_raise(u'foo/' + 251 * u'x', "segment too long")
        # So a segment of 125 two-byte chars plus one should also fail.
        self._should_raise(u'foo/' + 125 * TWO_BYTE_UNICHR + u'x', "segment too long")
