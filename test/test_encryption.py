#
# File: test_encryption.py
#
# Copyright 2016, Backblaze Inc.  All rights reserved.
#

import unittest

from b2.encryption import CryptoContext


class TestFileNameEncryption(unittest.TestCase):

    def setUp(self):
        self.context = CryptoContext()
        # TODO: set the master key on the context

    def test_hash_file_name(self):
        self.assertEqual(
            's4wjOOg8rKNB8Q_Aw1R4',
            self.context.hash_filename('photos')
        )
        self.assertEqual(
            's4wjOOg8rKNB8Q_Aw1R4/GkExLqMNCRKwEt4FWRla',
            self.context.hash_filename('photos/kittens')
        )
        self.assertEqual(
            's4wjOOg8rKNB8Q_Aw1R4/GkExLqMNCRKwEt4FWRla/y_izVpsP4IPwdvRflvWj',
            self.context.hash_filename('photos/kittens/fluffy.jpg')
        )

    def test_simpler_hash_file_name(self):
        # Prefixes should match
        self.assertEqual(
            u'wceNWSw5tecofhRWGHoVwfyH',
            self.context.simpler_hash_filename(u'salt', u'photos')
        )
        self.assertEqual(
            u'wceNWSw5tecofhRWGHoVwfyH/iSzIV33pwz3EDJX107hYW7Qo',
            self.context.simpler_hash_filename(u'salt', u'photos/kittens')
        )
        self.assertEqual(
            u'wceNWSw5tecofhRWGHoVwfyH/iSzIV33pwz3EDJX107hYW7Qo/sQeDvj-tWpqJLzY8mawDkkvf',
            self.context.simpler_hash_filename(u'salt', u'photos/kittens/fluffy.jpg')
        )

        # 'kittens' inside a different folder should have a different hash value.
        self.assertEqual(
            u'AAn4mwGKjw2R-w8CKpdByQz5/x51emS08_pdQfJNgEuN5motv',
            self.context.simpler_hash_filename(u'salt', u'movies/kittens')
        )


    def test_short_file_name(self):
        self.assertEqual('GKpwAuz7JUaS2JLBLKW0/UVeMie+SQ+1pTjEUCxGBBg=', self.context.encrypt_filename('a'))

    def test_long_file_name(self):
        cipher = 'P10p6qSLiG7OmM+pyITvSF+dTLI4zvkJBKQGVRTQTSL7ZW4NByDULY5y0XjNqyItkwfSqWQZtoWjUQy0RVHV9m64l+9JN6rJFpH1EHqwUCF/t9QWu7G/eoTUBd5tw5qdhj9mxvp243R1p+vfKtlXZBYqkuZDpE1KAepR5HUJyvk53MFNjRClXoN2CfbwYKM6'
        self.assertEqual(192, len(cipher))
        self.assertEqual(cipher, self.context.encrypt_filename('a' * 112))

    def _check_encrypt_file_name(self, plain, cipher):
        self.assertEqual(cipher, self.context.encrypt_filename(plain))
        self.assertEqual(plain, self.context.decrypt_filename(cipher))


