# Encryption of Files in B2

This document outlines the goals and the design of encryption of files
in B2, along with the reasons for making various choices

## Goals

### Goals

- In command-line tool, store passphrase in `.b2_account_info`, so the user does not have to entry it each time.
- Encrypt file names and metadata (file info).
- Support for the sync command for encrypted buckets.
- B2 service verifies SHA1 checksum of encrypted data on upload.
- SHA1 checksum of original data stored with file, so that original content can be verified after downloading.
- Support for changing passphrase without re-uploading everything.
- Checking if a file with a certain name is in the bucket should be efficient
- "file info" metadata must be encrypted
- Max file name length should not be decreased

### Non-Goals

These are things that we have explicitly decided not to do, at least
for now:

- Per-file encryption status.  For now, the encryption settings are on the bucket.

### Simplifying Assumptions

???

## Metadata and File Format

Encryption is specified per bucket.  Each bucket has its own master key.

The master key for a bucket is stored in a file called `.MASTER_KEY` in the bucket.  This file contains a JSON with this information:

- ???

### Per-file Key Generation

A different key used to encrypt each file.  The files are encrypted
with 128-bit AES, so each key is 128 bits, or 16 bytes. The key is
created using [PBKDF2](https://en.wikipedia.org/wiki/PBKDF2) with
SHA256 as the hash function and 500000 Iterations.

The Python code to create a key looks like this, using the
cryptography library:

    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=16,
        salt=salt,
        iterations=500000,
        backend=default_backend()
    )
    file_key = kdf.derive(passphrase)

### Per-file Initialization Vector

Each file gets its own 16-byte initialization vector, which is
randomly generated, and then stored in the header on the file.

### Encrypted File Format

Each encrypted file is prefixed with a header before being stored.
The header contains:

- The salt used to generate the key for the file
- The initialization vector for the file
- The number of sections (blocks) in the file.

### File Encryption

Files are encrypted in sections, each on NNN bytes long.
