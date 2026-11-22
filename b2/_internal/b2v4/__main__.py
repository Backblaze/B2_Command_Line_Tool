######################################################################
#
# File: b2/_internal/b2v4/__main__.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2._internal._cli.autocomplete_cache import initialize_autocomplete

initialize_autocomplete('b2v4')

from b2._internal.b2v4.registry import main  # noqa: E402

main()
