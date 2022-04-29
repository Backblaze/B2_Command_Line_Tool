.. _replication:

########################
Replication
########################

***********************
Automatic setup
***********************

Setup replication
=================

.. code-block:: sh

    $ b2 replication-setup --destination-profile myprofile2 my-source-bucket my-destination-bucket

You can optionally choose source rule priority and source rule name. See :ref:`replication-setup command <replication_setup_command>`.

.. note::
   ``replication-setup`` will reuse or provision a source key with no prefix and full reading capabilities and a destination key with no prefix and full writing capabilities

***************
Manual setup
***************

Setup source key
================

.. code-block:: sh

    $ b2 create-key my-source-bucket-rplsrc readFiles,readFileLegalHolds,readFileRetentions
    0014ab1234567890000000123 K001ZA12345678901234567890ABCDE


Setup destination key
=====================

.. code-block:: sh

    $ b2 create-key --profile myprofile2 my-destination-bucket-rpldst writeFiles,writeFileLegalHolds,writeFileRetentions,deleteFiles
    0024ab2345678900000000234 K001YYABCDE12345678901234567890


Setup source replication
========================

.. code-block:: sh

    $ b2 update-bucket --replication '
    "asReplicationSource": {
        "replicationRules": [
            {
                "destinationBucketId": "85644d98debc657d880b0e1e",
                "fileNamePrefix": "files-to-share/",
                "includeExistingFiles": false,
                "isEnabled": true,
                "priority": 128,
                "replicationRuleName": "my-replication-rule-name"
            }
        ],
        "sourceApplicationKeyId": "0014ab1234567890000000123"
    }' my-source-bucket

Setup destination replication
=============================

.. code-block:: sh

    $ b2 update-bucket --profile myprofile2 --replication '
    "asReplicationDestination": {
        "sourceToDestinationKeyMapping": {
            "0014ab1234567890000000123": "0024ab2345678900000000234"
        }
    }' my-destination-bucket
