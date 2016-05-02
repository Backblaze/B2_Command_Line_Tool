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





