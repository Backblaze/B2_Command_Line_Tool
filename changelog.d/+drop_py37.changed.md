Remove Python 3.7 support in new releases.
Under Python 3.7 `pip` will keep resolving the latest version of the package that supports active interpreter.
This change comes at benefit of using newer versions of B2 CLI dependencies in `b2` standalone binary as well as in the official docker image.
Python 3.8 is now the minimum supported version, until it reaches EOL in October 2024.
