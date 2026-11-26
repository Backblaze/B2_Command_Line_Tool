Scripting and agents: output, exit codes and streams
====================================================

Choose an interface
-------------------

Use ``b2v5`` for scripts and agents. The unversioned ``b2`` command selects
whichever ApiVer is currently stable; a future release can select another one.
ApiVer is the compatibility boundary for command syntax, machine-readable output
and exit-code contracts. Version-bound interfaces still receive bug and security
fixes, so do not depend on a known bug remaining unchanged. Human-readable
``replication status`` console output is explicitly subject to change.

Output formats today
--------------------

The following describes successful commands in ApiVer 5, before applying
``--quiet``. Empty collections or operations with nothing to report may produce
no text. JSON schemas differ by command; do not assume that every JSON object
has the same fields or matches a server response exactly.

.. list-table:: Result formats
   :header-rows: 1
   :widths: 48 52

   * - Command
     - Output on stdout
   * - ``account authorize``, ``account get``, ``bucket get``
     - JSON by default.
   * - ``file info``, ``file hide``, ``file unhide``, ``file server-side-copy``
     - JSON by default.
   * - ``ls``, ``bucket list``, ``bucket notification-rule list``
     - Human-readable text by default; ``--json`` selects JSON.
   * - ``key list``
     - Text; there is no ``--json`` option.
   * - ``bucket create``
     - Bucket ID as text.
   * - ``key create``, ``key delete``
     - Text results.
   * - ``file url``
     - URL as text.
   * - ``file cat``; ``file download`` with destination ``-``
     - File bytes, without download metadata or completion messages.
   * - ``file download`` to a local path
     - Human-readable metadata and completion messages; file bytes go to that path.
   * - ``file upload``
     - Two URL lines followed by JSON. The entire stdout stream is not a JSON document.
   * - ``file large unfinished list``, ``file large parts``
     - Text rows.
   * - ``rm``, ``sync``
     - Human-readable reporting; a successful operation can also be silent.
   * - ``replication status``
     - Human-readable console output by default. Select JSON or CSV with
       ``--output-format json`` or ``--output-format csv``.
   * - ``replication setup``, ``replication pause``, ``replication unpause``,
       ``replication delete``
     - Silent on success.

Exit codes
----------

Check the exit status before consuming stdout as a complete result.

.. list-table:: Exit status
   :header-rows: 1

   * - Code
     - Meaning
   * - 0
     - Successful command or an explicit help request. A handled broken output
       pipe also returns 0; it does not prove that the downstream consumer read
       all the data.
   * - 1
     - Handled command failure, including SDK errors, command validation errors,
       missing account data or failed authorization. Also returned when ``rm``
       reports a failed removal, ApiVer 5 ``sync`` reports errors or warnings,
       or Ctrl-C is caught during command execution. An uncaught Python
       exception normally prints a traceback and terminates with status 1.
   * - 2
     - Argument-parser usage error, such as an unknown option or missing argument.

These are CLI statuses, not HTTP status codes. An operating-system termination
or a signal outside the command's interrupt handler need not use this table.
A nonzero status can follow partial work; it does not mean that nothing changed.

Streams and progress
--------------------

* Results normally go to stdout. Upload and download progress goes to stderr.
* ``rm`` and ``sync`` reporting, including their progress and operation-error
  reports, uses stdout. General handled command errors go to stderr.
* ``replication status`` progress also uses stdout. With JSON or CSV, pass
  ``--no-progress`` to keep progress out of the result. CSV header cells can
  contain embedded newlines and CSV output ignores ``--quiet``. Use a real CSV
  reader rather than splitting lines. These are current behaviors tracked in
  `#1293 <https://github.com/Backblaze/B2_Command_Line_Tool/issues/1293>`_.
* Command warnings and diagnostics can appear on stderr. Optional logging can
  add diagnostics, so capture stderr separately from result data.
* Explicit ``--help`` writes help to stdout and exits 0. On usage errors, help
  currently also goes to stdout, with the error message on stderr and exit 2.
  See `#1156 <https://github.com/Backblaze/B2_Command_Line_Tool/pull/1156>`_.

``--no-progress`` disables progress displays on commands that support it; it
does not suppress result data or every informational line. It is preferable to
``--quiet`` when a script needs a plain-text result.

``--quiet`` suppresses ordinary text printed through the command's stdout
printer, including some data results. JSON output and command stderr are forced
through, and replication CSV has its own stdout path. Do not use quiet mode as
a general way to discard diagnostics or as a guarantee that all data is retained.
The plain-text data suppression is tracked in
`#1291 <https://github.com/Backblaze/B2_Command_Line_Tool/issues/1291>`_.

On current versions, ``ls --json --quiet`` includes the JSON brackets and
commas; the punctuation bug in
`#1215 <https://github.com/Backblaze/B2_Command_Line_Tool/issues/1215>`_ is fixed.
The listing also closes a started array when listing fails, unlike the historical
report in `#1212 <https://github.com/Backblaze/B2_Command_Line_Tool/issues/1212>`_.
Such output can contain only a partial listing, and failure before listing begins
can produce no JSON. Always check the exit status, even if stdout parses as JSON.

Control characters
------------------

By default, text output escapes control characters when stdout is a terminal.
For redirected text output, ``B2_ESCAPE_CONTROL_CHARACTERS=1`` enables escaping
and ``B2_ESCAPE_CONTROL_CHARACTERS=0`` disables it. An explicit
``--escape-control-characters`` or ``--no-escape-control-characters`` option takes
precedence over the environment. An invalid environment value falls back to
terminal detection; a warning is logged and is subject to logging configuration.
These settings apply to text rendering, not to file bytes written by a download
or ``file cat``. JSON output already escapes string control characters.

Examples captured offline
-------------------------

These commands were run through the unit-test command helper with b2sdk's
``RawSimulator``. All values are simulator fixtures. Output blocks are copied
from those runs; identifiers are not values to expect from a live account.

Plain text: create an empty fixture bucket.

.. code-block:: sh

   b2v5 bucket create example-scripts allPrivate

.. code-block:: text

   bucket_0

An opt-in JSON result: list the still-empty bucket.

.. code-block:: sh

   b2v5 ls --json b2://example-scripts/

.. code-block:: json

   []

A JSON-by-default result: remove the hide marker from the fixture object
``example.txt`` after uploading and hiding it in the simulator. The ``delete``
action refers to removal of that marker.

.. code-block:: sh

   b2v5 file unhide b2://example-scripts/example.txt

.. code-block:: json

   {
       "action": "delete",
       "fileId": "9998",
       "fileName": "example.txt"
   }

Upload output: the fixture file contains ``example content`` followed by a
newline, with a fixed modification time. The URL lines precede the JSON.

.. code-block:: sh

   b2v5 file upload --no-progress example-scripts example.txt example.txt

.. code-block:: text

   URL by file name: http://download.example.com/file/example-scripts/example.txt
   URL by fileId: http://download.example.com/b2api/v4/b2_download_file_by_id?fileId=9999
   {
       "accountId": "account-0",
       "action": "upload",
       "bucketId": "bucket_0",
       "contentSha1": "a943c7594cae5d4efad6ec2fbc442def5056b170",
       "contentType": "b2/x-auto",
       "fileId": "9999",
       "fileInfo": {
           "src_last_modified_millis": "1500111222000"
       },
       "fileName": "example.txt",
       "fileRetention": {
           "mode": null,
           "retainUntilTimestamp": null
       },
       "legalHold": null,
       "replicationStatus": null,
       "serverSideEncryption": {
           "algorithm": "AES256",
           "mode": "SSE-B2"
       },
       "size": 16,
       "uploadTimestamp": 5000
   }

CSV output: the simulator has one source object and an ``example-rule``
replication rule. The destination scan is disabled.

.. code-block:: sh

   b2v5 replication status --no-progress --dont-scan-destination --output-format csv example-scripts

.. code-block:: text

   rule name,"source
   replication
   status","source
   has
   hide
   marker","source
   encryption
   mode","source
   has
   large
   metadata","source
   has
   file
   retention","source
   has
   legal
   hold","destination
   replication
   status","metadata
   differs","hash
   differs",count
   example-rule,,No,SSE_B2,No,No,No,,,,1

The multi-line CSV headers are tracked in `#1293 <https://github.com/Backblaze/B2_Command_Line_Tool/issues/1293>`_.
