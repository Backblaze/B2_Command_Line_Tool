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

## Bucket Encryption Settings 

Encryption is specified per bucket.  Each bucket has its own master key.

The master key for a bucket is stored in a file called `.MASTER_KEY` in the bucket.  This file contains 
a JSON with this information:

- ???

## Per-File Information

### File Name

The names of encrypted files are themselves encrypted.  The file name used to store the file
in B2 is a path, with each segment of the path being a SHA-256 HMAC of the cleartext.  It 
is a one-way mapping from cleartext file name to the name used in the B2 bucket.  To get
the name of a file, you must decrypt the encrypted file name that's stored in the file 
info.

This path:

    photos/kittens/fluffy.jpeg
   
Could turn into this:

    s4wjOOg8rKNB8Q_Aw1R4/GkExLqMNCRKwEt4FWRla/y_izVpsP4IPwdvRflvWj

The grouping of files into folders is preserved by this mapping.  You can find all
of the files in a folder by hashing the folder path, and using it as a prefix when
listing the files from B2.

There are some valid B2 path names that cannot be used, because they would be too
long after being hashed.  The B2 limit of 1000 bytes means that encrypted path names can have 
at most 47 segments.

The file name is encrypted and stored in file info.  Because each file info value is limited to 200 bytes, 
the encryption block size is 16 bytes, the initialization vector is included in the encrypted name, and 
the resulting name is base-64 encoded, the maximum file name size is 112 bytes of UTF-8.

### File Info

The following file info is stored with each encrypted file, in addition to the whatever 
other file info the user wants to store.

- `encrypted_file_name` - the encrypted name of the file

(Note: code currently uses `name`, which is to common a name to use.)

## Encryption Process

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
