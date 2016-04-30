######################################################################
#
# File: b2/encryption.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os
import struct

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, hmac, padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import (Cipher, algorithms, modes)


def derive_key(passphrase, salt, iterations=500000):
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=16,
        salt=salt,
        iterations=iterations,
        backend=default_backend()
    )
    return kdf.derive(passphrase)


def pack_iv(iv):
    return struct.pack('>III', (iv >> 64) & 0xFFFFFFFF, (iv >> 32) & 0xFFFFFFFF, iv & 0xFFFFFFFF)


def unpack_iv(piv):
    iv = struct.unpack('>III', piv)
    return (iv[0] << 64) | (iv[1] << 32) | iv[2]


class EncryptingFileStream(object):
    def __init__(self, upload_source, crypto_file):
        self.stream = upload_source.open()
        self.crypto_file = crypto_file
        self.buffer = b''
        self.block = 0
        self.eof = False

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stream.close()
        return None  # don't hide exception

    def read(self, size):
        while not self.eof and len(self.buffer) < size:
            self.fillBuffer()
        rtn = self.buffer[0:size]
        self.buffer = self.buffer[size:]
        return rtn

    def seek(self, offset):
        # calculate positions
        header_size = len(self.crypto_file.header())
        auth_tag_size = self.crypto_file.auth_tag_size()
        block_size = self.crypto_file.crypto.block_size
        self.block = max(0, offset - header_size) // (block_size + auth_tag_size)
        if self.block == 0:
            block_offset = offset  # first block includes header
        else:
            block_offset = (offset - header_size) % (block_size + auth_tag_size)

        # refill buffer
        self.eof = False
        self.stream.seek(self.block * block_size)
        self.buffer = b''
        self.fillBuffer()
        self.buffer = self.buffer[block_offset:]

    def close(self):
        self.stream.close()

    def fillBuffer(self):
        # write header
        if self.block == 0:
            self.buffer = self.crypto_file.header()

        # write encrypted data
        block_size = self.crypto_file.crypto.block_size
        data = self.stream.read(block_size)
        if len(data) < block_size:
            self.eof = True
        self.buffer += self.crypto_file.encrypt_block(self.block, data)
        self.block += 1


class FileEncryptionContext(object):
    def __init__(self, crypto, file_size):
        self.crypto = crypto
        self.file_size = file_size
        self.salt = os.urandom(16)
        self.iv = os.urandom(12)
        self.blocks = struct.pack('>Q', crypto.block_count(file_size))
        self.file_key = derive_key(crypto.get_master_key(), self.salt, 1)

    def header(self):
        return self.salt + self.blocks + self.iv

    def encrypt_block(self, block_id, data):
        iv_int = unpack_iv(self.iv)
        encryptor = Cipher(
            algorithms.AES(self.file_key),
            modes.GCM(pack_iv(iv_int + block_id)),
            backend=default_backend()
        ).encryptor()
        encryptor.authenticate_additional_data(self.blocks)
        ciphertext = encryptor.update(data) + encryptor.finalize()
        return ciphertext + encryptor.tag

    def auth_tag_size(self):
        return 16

    def encrypted_size(self):
        auth_size = self.crypto.block_count(self.file_size) * self.auth_tag_size()
        return self.file_size + len(self.header()) + auth_size


class EncryptionContext(object):
    def __init__(self):
        self.master_key = b'bla'
        self.block_size = 16 * 1024

    def make_file_context(self, file_size):
        return FileEncryptionContext(self, file_size)

    def get_master_key(self):
        return self.master_key

    def block_count(self, file_size):
        return (file_size + self.block_size - 1) // self.block_size  # divide rounding up

    def encrypt_filename(self, filename, folder=''):
        # derive multiple keys
        enc_key = derive_key(self.master_key, b'filename', 1)
        hmac_key = derive_key(self.master_key, b'filename_hmac', 1)

        # generate IV from filename HMAC
        h = hmac.HMAC(hmac_key, hashes.SHA256(), backend=default_backend())
        h.update(folder + filename)
        iv = h.finalize()[0:16]

        # encrypt filename
        encryptor = Cipher(
            algorithms.AES(enc_key),
            modes.CBC(iv), backend=default_backend()
        ).encryptor()
        padder = padding.PKCS7(128).padder()
        filenamePadded = padder.update(filename) + padder.finalize()
        return iv + encryptor.update(filenamePadded) + encryptor.finalize()

    def decrypt_filename(self, filename):
        # get key and iv
        enc_key = derive_key(self.master_key, b'filename', 1)
        iv = filename[0:16]
        ciphertext = filename[16:]

        # decrypt filename
        decryptor = Cipher(
            algorithms.AES(enc_key),
            modes.CBC(iv), backend=default_backend()
        ).decryptor()
        unpadder = padding.PKCS7(128).unpadder()
        plaintextPadded = decryptor.update(ciphertext) + decryptor.finalize()
        return unpadder.update(plaintextPadded) + unpadder.finalize()
