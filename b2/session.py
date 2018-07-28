######################################################################
#
# File: b2/session.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import functools

from b2.exception import (InvalidAuthToken, Unauthorized)
from b2.raw_api import ALL_CAPABILITIES


class B2Session(object):
    """
        A *magic* facade that supplies the correct api_url and account_auth_token
        to methods of underlying raw_api and reauthorizes if necessary
    """

    def __init__(self, api, raw_api):
        self._api = api  # for reauthorization
        self.raw_api = raw_api

    def __getattr__(self, name):
        f = getattr(self.raw_api, name)

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            auth_failure_encountered = False
            # download_by_name uses different URLs
            url_factory = kwargs.pop('url_factory', self._api.account_info.get_api_url)
            while 1:
                api_url = url_factory()
                account_auth_token = self._api.account_info.get_account_auth_token()
                try:
                    return f(api_url, account_auth_token, *args, **kwargs)
                except InvalidAuthToken:
                    if not auth_failure_encountered:
                        auth_failure_encountered = True
                        reauthorization_success = self._api.authorize_automatically()
                        if reauthorization_success:
                            continue
                        # TODO: exception chaining could be added here
                        #       to help debug reauthorization failures
                    raise
                except Unauthorized as e:
                    raise self._add_app_key_info_to_unauthorized(e)

        return wrapper

    def _add_app_key_info_to_unauthorized(self, unauthorized):
        """
        Takes an Unauthorized error and adds information from the application key
        about why it might have failed.
        """
        # What's allowed?
        allowed = self._api.account_info.get_allowed()
        capabilities = allowed['capabilities']
        bucket_name = allowed['bucketName']
        name_prefix = allowed['namePrefix']

        # Make a list of messages about the application key restrictions
        key_messages = []
        if set(capabilities) != set(ALL_CAPABILITIES):
            key_messages.append("with capabilities '" + ','.join(capabilities) + "'")
        if bucket_name is not None:
            key_messages.append("restricted to bucket '" + bucket_name + "'")
        if name_prefix is not None:
            key_messages.append("restricted to files that start with '" + name_prefix + "'")
        if not key_messages:
            key_messages.append('with no restrictions')

        # Make a new message
        new_message = unauthorized.message
        if new_message == '':
            new_message = 'unauthorized'
        new_message += ' for application key ' + ', '.join(key_messages)

        return Unauthorized(new_message, unauthorized.code)
