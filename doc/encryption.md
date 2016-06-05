# Encryption of Files in B2

This document outlines the goals and the design of encryption of files
in B2, along with the reasons for making various choices

## Goals

### Goals

- Encrypt the content of files.
- Encrypt the names of files.
- Support for the sync command for encrypted buckets.
- B2 service verifies SHA1 checksum of encrypted data on upload.
- SHA1 checksum of original data stored with file, so that original content can be verified after downloading.
- Be able to change the passphrase for a bucket without re-uploading everything.
- Finding a file by name in a bucket is efficient.
- Listing the names all of the files in a folder is efficient.  (Listing just some of the files in a folder need not be any faster.)
- Max file name length should not be decreased
- In command-line tool, store passphrase in `.b2_account_info`, so the user does not have to entry it each time.

### Non-Goals

These are things that we have explicitly decided not to do, at least
for now:

- Per-file encryption status.  For now, the encryption settings are on the bucket.
- Encrypting the file info that is stored with a file.

### Simplifying Assumptions

???

## Bucket Encryption Settings 

Encryption is specified per bucket.  When a bucket is encrypted, it
contains a file called `.MASTER_KEY`, which holds the salt used
to generate encryption keys.  Keys are created based on the salt,
and on the user's secret passphrase.

The file contains:

- The 16-byte salt used to generate encryption keys, as hex.
- The 16-byte salt used to generate file name encryption keys.

The format looks like this:

    {
        "encryptionKeySalt" : "7b536cce8a053634be024d53f2dcaebb"
    }

## Per-File Information

### Encrypted File Name

The file name for each file is encrypted using the same algorithm 
and the same key as the contents of the file.  
See the "Encryption Process" section below.

The result is a random initialization vection (I.V.), followed by the
encrypted data.  This whole thing is base64 encoded (using "url-safe"
encoding), and stored in the file info as `encrypted_file_name`.

*QUESTION*: Why does the file name encryption use CBC, while the 
file encryption uses GCM?

*QUESTION*: What's the reason for choosing CBC or GCM?

### B2 File Name

When the encrypted content is stored in B2, the file needs a name.
The original file name is not used, because it may contain information
that should be secret.  The encrypted name is not used because it
may be too long, and because it does not reflect the folder structure.

To accomplish this, hashes are used for each folder and file name
stored.  If you store the file `photos/kittens/fluffy.jpg`, each of
these strings is hashed:

- `photos`
- `photos/kittens`,
- `photos/kittens/fluffy.jpg`

The hash of each part includes the parts that came before, so that 
attackers cannot see that folders in different parts of the tree
have the same name.  `photos/kittens` and `videos/kittens` hash to
different values, so you cannot see that they are both called `kittens`.

The hash for each level of folders, and the full name are joined with
slashes to produce the name used to store the file.  The file 
`photos/kittens/fluffy.jpg` might have this path:

    s4wjOOg8rKNB8Q_Aw1R4/GkExLqMNCRKwEt4FWRla/y_izVpsP4IPwdvRflvWj

The salt used when hashing file names must be the same for all 
files in the bucket, so that files in the same folder hash to
files in the same folder.  The salt should be secret, so that 
even if the `.MASTER_KEY` file is compromised, an attacker will
not have it.

The file hashing salt is created using the same algorithm used to generate
encryption keys, but uses `/make_salt` as the file name, which
is not a valid B2 file name because it starts with `/`.
(For details, see below for Per-file Key Generation.)

- Append the salt to the front of the UTF-8 bytes of the name to be hashed.
- Compute the SHA1 of salt + name.
- Base64-encode the first 18 bytes fo the SHA1 digest, resulting in a 24-byte hash.

A full HMAC is not needed for this hashing, because we do not need to
defend against attackers trying to create fake messages.  A simple hash
is sufficient.

*QUESTION*: Are we OK with SHA1, or is HMAC worth the complexity?

There are some valid B2 path names that cannot be used, because they would be too
long after being hashed.  The B2 limit of 1000 bytes means that encrypted path 
names can have at most 40 segments (the parts in between slashes).

### File Info

The following file info is stored with each encrypted file, in addition to the whatever 
other file info the user wants to store.

- `encrypted_format` - Always set to `PBKDF2_AES_CBC`
- `encrypted_file_name` - the encrypted name of the file

*QUESTION*: CBC or GCM?

(Note: code currently uses `name`, which is to common a name to use.)

## Encryption Process

### Passphrase

The same passphrase is used for all files in a bucket.  It is combined
with a unqiue salt for each file to produce the AES key to use for that
file.

### Per-file Key Generation

A different key used to encrypt each file.  The files are encrypted
with 128-bit AES, so each key is 128 bits, or 16 bytes. The key is
created using [PBKDF2](https://en.wikipedia.org/wiki/PBKDF2) with
SHA256 as the hash function and 500000 Iterations.

*QUESTION*: How many iterations should we use?  The default in
`derive_key` is 500000, but the code is currently calling it with 1.

The Python code to create a key looks like this, using the
cryptography library:

    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=16,
        salt=bucket_key_generation_salt,
        iterations=???,  # TODO: How many?
        backend=default_backend()   # TODO: specify backend
    )
    file_key = kdf.derive(passphrase)

*TODO*: Specify which backend to use, in case the library changes
what the default is.

### Per-file Initialization Vector

Each file gets its own 16-byte initialization vector, which is
randomly generated, and then stored in the header on the file.

### Encrypted File Format

Each encrypted file is prefixed with a header before being stored.
The header contains:


### File Encryption

Files are encrypted in sections, each on 16384 bytes (16 KiB) long.  We call each
of these sections a "chapter", to avoid confusing them with the AES 
"blocks", each of which is 16 bytes.

A file to be encrypted is broken into chapters, with all but the last being 
exactly 16 KiB, and the last one being at least one byte, and at most 16 KiB.

Each of the chapters is encrypted with AES128/GCM.

### File Format

The encrypted file is the concatenation of:

- The string "AA_b2_crypt_0_AA" (16 bytes)
- The salt used to generate the key for the file (16 bytes)
- The initialization vector for the first chapter of the file (12 bytes)
- The number of chapters in the file (8 ascii hex digits in 8 bytes)
- The binary SHA1 digest of everything up to this point. (20 bytes)
- chapter 0, encrypted
- chapter 1, encrypted
- ...
- last chapter, encrypted
- the string "ZZ_b2_crypt_0_ZZ" (16 bytes)

The header is the concatenation of:

- The salt used to generate the key for the file (16 bytes)
- The initialization vector for the first chapter of the file (12 bytes)
- The number of chapters in the file.

*PROPOSAL*: Start with a constant string as a double check that the format is correct.

*PROPOSAL*: Include a SHA1 hash of the header.

*PROPOSAL*: Store the number of chapters in hex, not binary.  It's easier to deal with in some languages.

*QUESTION*: Why is the IV only 12 bytes?

### Encryption

The AES key for every chapter of a file is the same (see above), 
but the initialization vector is different.  
The initialization vector for a chapter is computed by
treating the file's I.V. as a big-endian integer, and adding the zero-based
chapter index.  Chapter 0 uses the file's I.V..  Chapter 1 adds 1 to that, and
so on.

In Python, the encryption of a chapter looks like this:

    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    
    encryptor = Cipher(
        algorithms.AES(file_key),
        modes.GCM(chapter_initialization_vector),
        backend=default_backend()  # TODO: specify the backend, in case the default changes
    ).encryptor()
    encryptor.authenticate_additional_data(self.blocks)
    ciphertext = encryptor.update(data) + encryptor.finalize() + encryptor.tag
    assert len(ciphertext) == len(data) + 16
    
And decryption looks like this:

    decryptor = Cipher(
        algorithms.AES(file_key),
        modes.GCM(chapter_initializion_vector), tag),
        backend=default_backend()  # TODO: specify the backend, in case the default changes
    ).decryptor()
    decryptor.authenticate_additional_data(self.blocks)
    return decryptor.update(ciphertext) + decryptor.finalize()

*QUESTION*: What does `authenticate_additional_data` buy us?  Given that the IV for each
chapter is different, I would have expected that validating just the contents of the
chapter would be enough.
