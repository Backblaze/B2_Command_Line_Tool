Unit tests no longer swallow exceptions raised by commands that use the progress bar (the `_TqdmCloser.__exit__` mock returned a truthy value).
