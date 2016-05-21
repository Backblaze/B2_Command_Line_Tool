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
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


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
        self.upload_source = upload_source
        self.crypto_file = crypto_file

    def __enter__(self):
        self.stream = self.upload_source.open().__enter__()
        self.buffer = b''
        self.block = 0
        self.eof = False
        return self

    def __exit__(self, type_, value, traceback):
        self.stream.__exit__(type_, value, traceback)
        return None  # don't hide exception

    def read(self, size=None):
        if size is None:
            size = self.crypto_file.encrypted_size()
        while not self.eof and len(self.buffer) < size:
            self._fillBuffer()
        rtn = self.buffer[0:size]
        self.buffer = self.buffer[size:]
        return rtn

    def seek(self, offset):
        # calculate positions
        header_size = len(self.crypto_file.header())
        auth_tag_size = self.crypto_file.crypto.auth_tag_size
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
        self._fillBuffer()
        self.buffer = self.buffer[block_offset:]

    def _fillBuffer(self):
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
        self.blocks = struct.pack('>Q', self.block_count())
        self.file_key = derive_key(crypto.get_master_key(), self.salt, 1)

    def header(self):
        data = self.salt + self.blocks + self.iv
        assert (len(data) == self.crypto.header_size)
        return data

    def block_count(self):
        block_size = self.crypto.block_size
        return (self.file_size + block_size - 1) // block_size  # divide rounding up

    def encrypt_block(self, block_id, data):
        iv_int = unpack_iv(self.iv)
        encryptor = Cipher(
            algorithms.AES(self.file_key),
            modes.GCM(pack_iv(iv_int + block_id)),
            backend=default_backend()
        ).encryptor()
        encryptor.authenticate_additional_data(self.blocks)
        ciphertext = encryptor.update(data) + encryptor.finalize()
        assert (len(encryptor.tag) == self.crypto.auth_tag_size)
        return ciphertext + encryptor.tag

    def encrypted_size(self):
        auth_size = self.block_count() * self.crypto.auth_tag_size
        return self.file_size + self.crypto.header_size + auth_size


class DecryptingFileStream(object):
    def __init__(self, download_dest, params, crypto_file):
        self.download_dest = download_dest
        self.params = params
        self.crypto_file = crypto_file

    def __enter__(self):
        self.stream = self.download_dest.open(*self.params).__enter__()
        self.buffer = b''
        self.block = 0
        self.header_read = False
        self.bytes_processed = 0
        return self

    def __exit__(self, type_, value, traceback):
        self.stream.__exit__(type_, value, traceback)
        return None  # don't hide exception

    def write(self, bytes):
        self.buffer += bytes
        self.bytes_processed += len(bytes)
        while self._flushBuffer():
            pass

        # end of file
        if self.bytes_processed == self.crypto_file.file_size:
            self._flushBuffer(True)

    def _flushBuffer(self, eof=False):
        # read header
        header_size = self.crypto_file.crypto.header_size
        if self.header_read == False:
            if len(self.buffer) >= header_size:
                header = self.buffer[0:header_size]
                self.buffer = self.buffer[header_size:]
                self.crypto_file.decode_header(header)
                self.header_read = True
                return True

            # Don't decrypt blocks until header is read
            return False

        # decrypt block
        enc_block_size = self.crypto_file.crypto.block_size + self.crypto_file.crypto.auth_tag_size
        if len(self.buffer) >= enc_block_size or eof:
            data = self.buffer[0:enc_block_size]
            self.buffer = self.buffer[enc_block_size:]
            self.stream.write(self.crypto_file.decrypt_block(self.block, data))
            self.block += 1
            return True

        return False


class FileDecryptionContext(object):
    def __init__(self, crypto, file_size):
        self.crypto = crypto
        self.file_size = file_size

    def decode_header(self, header):
        assert (len(header) == self.crypto.header_size)
        self.salt = header[0:16]
        self.blocks = header[16:24]
        self.iv = header[24:36]
        self.file_key = derive_key(self.crypto.get_master_key(), self.salt, 1)

    def block_count(self):
        data_size = self.file_size - self.crypt.header_size
        block_size = self.crypto.block_size + self.crypto.auth_tag_size
        return (data_size + block_size - 1) // block_size  # divide rounding up

    def decrypt_block(self, block_id, data):
        # split data block
        ciphertext = data[:-self.crypto.auth_tag_size]
        tag = data[-self.crypto.auth_tag_size:]

        # decrypt
        iv_int = unpack_iv(self.iv)
        decryptor = Cipher(
            algorithms.AES(self.file_key),
            modes.GCM(pack_iv(iv_int + block_id), tag),
            backend=default_backend()
        ).decryptor()
        decryptor.authenticate_additional_data(self.blocks)
        return decryptor.update(ciphertext) + decryptor.finalize()


class CryptoContext(object):
    def __init__(self):
        self.master_key = b'bla'
        self.block_size = 16 * 1024
        self.header_size = 16 + 8 + 12
        self.auth_tag_size = 16

    def make_encryption_context(self, file_size):
        return FileEncryptionContext(self, file_size)

    def make_decryption_context(self, file_size):
        return FileDecryptionContext(self, file_size)

    def get_master_key(self):
        return self.master_key

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
