######################################################################
#
# File: b2/b2http.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import print_function

import json
import requests
import six
import socket

class WebApiError(Exception):
    """
    Holds information extracted from a call to a B2 API.

    When an error happens, B2 returns an error JSON that looks like this:

        {
            "status" : 400,
            "code" : "invalid_bucket_name",
            "message" : "bucket name is too long"
        }
    """
    def __init__(self, status, code, message):
        super(WebApiError, self).__init__(status, code, message)
        self.status = status
        self.code = code
        self.message = message

    def __repr__(self):
        return "WebApiError(%d, '%s', '%s')" % (self.status, self.code, self.message)

    def __eq__(self, other):
        return repr(self) == repr(other)

    def __ne__(self, other):
        return not self.__eq__(other)


def _print_exception(e, indent=''):
    """
    Used for debugging to print out nested exception structures.
    """
    print(indent + 'EXCEPTION', repr(e))
    print(indent + 'CLASS', type(e))
    for (i, a) in enumerate(e.args):
        print(indent + 'ARG %d: %s' % (i, repr(a)))
        if isinstance(a, Exception):
            _print_exception(a, indent + '        ')


def _translate_errors(fcn):
    """
    Calls the given function, turning any exception raised into a WebApiError.
    """
    try:
        response = fcn()
        if response.status_code not in [200, 206]:
            # Decode the error object returned by the service
            error = json.loads(response.content.decode('utf-8'))
            raise WebApiError(int(error['status']), error['code'], error['message'])
        return response

    except WebApiError:
        raise # pass through exceptions from just above

    except requests.ConnectionError as e0:
        e1 = e0.args[0]
        if isinstance(e1, requests.packages.urllib3.exceptions.MaxRetryError):
            msg = e1.args[0]
            if 'nodename nor servname provided, or not known' in msg:
                # Unknown host, or DNS failing.  In the context of calling
                # B2, this means that something is down between here and
                # Backblaze, so we treat it like 503 Service Unavailable.
                raise WebApiError(503, 'unknown_host', 'unable to locate host')
        elif isinstance(e1, requests.packages.urllib3.exceptions.ProtocolError):
            e2 = e1.args[1]
            if isinstance(e2, socket.error):
                if e2.args[1] == 'Broken pipe':
                    # Broken pipes are usually caused by the service rejecting
                    # an upload request for cause, so we use a 400 Bad Request
                    # code.
                    raise WebApiError(503, 'broken_pipe', 'unable to send entire request')
        raise WebApiError(503, 'connection_error', 'unable to connect')

    except Exception as e:
        # Don't expect this to happen.  To get lots of info for
        # debugging, call print_exception.
        # print_exception(e)
        raise WebApiError(500, 'unknown', repr(e))


def post(url, headers, data):
    """
    Posts to the given URL, translating any errors into a raised WebApiError.

    :param url: The URL to post to.
    :param headers: Headers to send.
    :param data: Data to send: should be bytes.
    :return: A requests object with the response.
    """
    return _translate_errors(lambda: requests.post(url, headers=headers, data=data))


def get(url, headers):
    """
    Posts to the given URL, translating any errors into a raised WebApiError.

    :param url: The URL to post to.
    :param headers: Headers to send.
    :return: A requests object with the response.
    """
    return _translate_errors(lambda: requests.get(url, headers=headers))


def _test():
    """
    Runs a few tests on error diagnosis.

    This test takes a while to run, and is not used in the automated tests
    during building.  Run the test by hand to exercise the code.  Be sure
    to run in both Python 2 and Python 3.
    """

    # Error from B2
    print('TEST: error object from B2')
    try:
        post('https://api.backblaze.com/b2api/v1/b2_get_file_info', {}, six.b('{}'))
        assert False, 'should have failed with bad json'
    except WebApiError as e:
        assert e == WebApiError(400, 'bad_json', 'required field fileId is missing')

    # Successful get
    print('TEST: get')
    r = get('https://api.backblaze.com/test/echo_zeros?length=10', {})
    assert r.status_code == 200
    assert r.content == six.b(chr(0) * 10)

    # Successful post
    print('TEST: post')
    r = post('https://api.backblaze.com/test/echo_zeros', {}, six.b(json.dumps(dict(length=10))))
    assert r.status_code == 200
    assert r.content == six.b(chr(0) * 10)

    # Unknown host
    print('TEST: unknown host')
    try:
        post('https://unknown.backblaze.com', {}, six.b(''))
        assert False, 'should have failed with unknown host'
    except WebApiError as e:
        assert e == WebApiError(503, 'unknown_host', 'unable to locate host')

    # Broken pipe
    print('TEST: broken pipe')
    try:
        post('https://api.backblaze.com/bad_url', {}, six.b(chr(0)) * 10000000)
        assert False, 'should have failed with broken pipe'
    except WebApiError as e:
        assert e == WebApiError(503, 'broken_pipe', 'unable to send entire request')

    # Generic connection error
    print('TEST: generic connection error')
    try:
        post('https://api.backblaze.com:6666/bad_port', {}, six.b(chr(0)) * 10000000)
        assert False, 'should have failed with broken pipe'
    except WebApiError as e:
        assert e == WebApiError(503, 'connection_error', 'unable to connect')

_test()
