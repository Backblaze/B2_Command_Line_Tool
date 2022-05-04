.. _quick_start:

########################
Quick Start Guide
########################

.. _prepare_b2cli:

***********************
Prepare B2 cli
***********************

.. code-block:: sh

    $ b2 authorize-account 4ab123456789 001aabbccddeeff123456789012345678901234567
    Using https://api.backblazeb2.com

.. tip::
   Get credentials from `B2 website <https://secure.backblaze.com/user_signin.htm>`_

.. warning::
   Local users might be able to access your process list and read command arguments. To avoid exposing credentials,
   you can provide application key ID and application key using environment variables ``B2_APPLICATION_KEY_ID`` and ``B2_APPLICATION_KEY`` respectively.
   Those will be picked up automatically, so after defining those you'll just need to run ``b2 authorize-account`` with no extra parameters.

   .. code-block:: sh

      $ export B2_APPLICATION_KEY_ID="$(<file-with-key-id.txt)"
      $ export B2_APPLICATION_KEY="$(<file-with-key.txt)"
      $ b2 authorize-account
      Using https://api.backblazeb2.com


***************
Synchronization
***************

.. code-block:: sh

    $ b2 sync "/home/user1/b2_example" "b2://bucket1/example-mybucket-b2"

.. tip:: Sync is the preferred way of getting data into and out of B2 cloud, because it can achieve *highest performance* due to parallelization of scanning and data transfer operations.


**************
Bucket actions
**************

List buckets
============

.. code-block:: sh

    $ b2 list-buckets
    34567890abcdef1234567890  allPublic   example-mybucket-b2-1
    345678901234567890abcdef  allPublic   example-mybucket-b2-2

Create a bucket
===============

.. code-block:: sh

    $ b2 create_bucket example-mybucket-b2-3 allPublic
    ...

You can optionally store bucket info, CORS rules and lifecycle rules with the bucket.


Delete a bucket
===============

.. code-block:: sh

    $ b2 delete-bucket 'example-mybucket-b2-1'

returns 0 if successful, outputs a message and a non-0 return code in case of error.
