.. _replication:

########################
Replication
########################

If you have access to accounts hosting both source and destination bucket (it can be the same account), we recommend using ``replication-setup`` command described below. Otherwise use :ref:`manual setup <replication_manual_setup>`.

***********************
Automatic setup
***********************

Setup replication
=================

.. code-block:: sh

    $ b2 replication-setup --destination-profile myprofile2 my-bucket my-bucket2

You can optionally choose source rule priority and source rule name. See :ref:`replication-setup command <replication_setup_command>`.

.. note::
   ``replication-setup`` will reuse or provision a source key with no prefix and full reading capabilities and a destination key with no prefix and full writing capabilities

.. _replication_manual_setup:

***************
Manual setup
***************

Setup source key
================

.. code-block:: sh

    $ b2 create-key my-bucket-rplsrc readFiles,readFileLegalHolds,readFileRetentions
    0014ab1234567890000000123 K001ZA12345678901234567890ABCDE


Setup source replication
========================

.. code-block:: sh

    $ b2 update-bucket --replication '{
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
        }
    }' my-bucket


Setup destination key
=====================

.. code-block:: sh

    $ b2 create-key --profile myprofile2 my-bucket-rpldst writeFiles,writeFileLegalHolds,writeFileRetentions,deleteFiles
    0024ab2345678900000000234 K001YYABCDE12345678901234567890


Setup destination replication
=============================

.. code-block:: sh

    $ b2 update-bucket --profile myprofile2 --replication '{
        "asReplicationDestination": {
            "sourceToDestinationKeyMapping": {
                "0014ab1234567890000000123": "0024ab2345678900000000234"
            }
        }
    }' my-bucket
