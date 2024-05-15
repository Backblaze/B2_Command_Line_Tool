Fix `-` handling in file upload commands - even if file with `-` name exists, the stdin will be chosen over it.
This change affects `b2v4` (which is also aliased as `b2`), but not `b2v3` to keep backwards compatibility.
