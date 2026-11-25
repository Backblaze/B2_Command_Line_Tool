######################################################################
#
# File: b2/_internal/b2v5/__main__.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2._internal._cli.autocomplete_cache import initialize_autocomplete

initialize_autocomplete('b2v5')

from b2._internal.b2v5.registry import main  # noqa: E402

main()
