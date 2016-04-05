######################################################################
#
# File: test/test_b2http.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2.b2http import _translate_and_retry, _translate_errors, B2Http
from b2.exception import BadJson, BrokenPipe, ConnectionError, ServiceError, UnknownError, UnknownHost
from b2.version import USER_AGENT
import requests
import six
import socket
import sys
import unittest

if sys.version_info < (3, 3):
    from mock import call, MagicMock, patch
else:
    from unittest.mock import call, MagicMock, patch

IS_27_OR_LATER = sys.version_info[0] >= 3 or (sys.version_info[0] == 2 and sys.version_info[1] >= 7)


class TestTranslateErrors(unittest.TestCase):
    def test_ok(self):
        if IS_27_OR_LATER:
            response = MagicMock()
            response.status_code = 200
            actual = _translate_errors(lambda: response)
            self.assertIs(response, actual)

    def test_partial_content(self):
        if IS_27_OR_LATER:
            response = MagicMock()
            response.status_code = 206
            actual = _translate_errors(lambda: response)
            self.assertIs(response, actual)

    def test_b2_error(self):
        if IS_27_OR_LATER:
            response = MagicMock()
            response.status_code = 503
            response.content = six.b('{"status": 503, "code": "server_busy", "message": "busy"}')
            with self.assertRaises(ServiceError):
                _translate_errors(lambda: response)

    def test_broken_pipe(self):
        if IS_27_OR_LATER:

            def fcn():
                raise requests.ConnectionError(
                    requests.packages.urllib3.exceptions.ProtocolError(
                        "dummy", socket.error(20, 'Broken pipe')
                    )
                )
            with self.assertRaises(BrokenPipe):
                _translate_errors(fcn)

    def test_unknown_host(self):
        if IS_27_OR_LATER:

            def fcn():
                raise requests.ConnectionError(
                    requests.packages.urllib3.exceptions.MaxRetryError(
                        'AAA nodename nor servname provided, or not known AAA', 'http://example.com'
                    )
                )
            with self.assertRaises(UnknownHost):
                _translate_errors(fcn)

    def test_connection_error(self):
        if IS_27_OR_LATER:

            def fcn():
                raise requests.ConnectionError('a message')
            with self.assertRaises(ConnectionError):
                _translate_errors(fcn)

    def test_unknown_error(self):
        if IS_27_OR_LATER:

            def fcn():
                raise Exception('a message')
            with self.assertRaises(UnknownError):
                _translate_errors(fcn)


class TestTranslateAndRetry(unittest.TestCase):
    def setUp(self):
        if IS_27_OR_LATER:
            self.response = MagicMock()
            self.response.status_code = 200

    def test_works_first_try(self):
        if IS_27_OR_LATER:
            fcn = MagicMock()
            fcn.side_effect = [self.response]
            self.assertIs(self.response, _translate_and_retry(fcn, 3))

    def test_non_retryable(self):
        if IS_27_OR_LATER:
            with patch('time.sleep') as mock_time:
                fcn = MagicMock()
                fcn.side_effect = [BadJson('a'), self.response]
                with self.assertRaises(BadJson):
                    _translate_and_retry(fcn, 3)
                self.assertEqual([], mock_time.mock_calls)

    def test_works_second_try(self):
        if IS_27_OR_LATER:
            with patch('time.sleep') as mock_time:
                fcn = MagicMock()
                fcn.side_effect = [ServiceError('a'), self.response]
                self.assertIs(self.response, _translate_and_retry(fcn, 3))
                self.assertEqual([call(1.0)], mock_time.mock_calls)

    def test_never_works(self):
        if IS_27_OR_LATER:
            with patch('time.sleep') as mock_time:
                fcn = MagicMock()
                fcn.side_effect = [
                    ServiceError('a'), ServiceError('a'), ServiceError('a'), self.response
                ]
                with self.assertRaises(ServiceError):
                    _translate_and_retry(fcn, 3)
                self.assertEqual([call(1.0), call(1.5)], mock_time.mock_calls)


class TestB2Http(unittest.TestCase):

    URL = 'http://example.com'
    HEADERS = dict(my_header='my_value')
    EXPECTED_HEADERS = {'my_header': 'my_value', 'User-Agent': USER_AGENT}
    PARAMS = dict(fileSize=100)
    PARAMS_JSON_BYTES = six.b('{"fileSize": 100}')

    def setUp(self):
        self.requests = MagicMock()
        self.response = MagicMock()
        self.b2_http = B2Http(self.requests)

    def test_post_json_return_json(self):
        self.requests.post.return_value = self.response
        self.response.status_code = 200
        self.response.content = six.b('{"color": "blue"}')
        response_dict = self.b2_http.post_json_return_json(self.URL, self.HEADERS, self.PARAMS)
        self.assertEqual({'color': 'blue'}, response_dict)
        (pos_args, kw_args) = self.requests.post.call_args
        self.assertEqual(self.URL, pos_args[0])
        self.assertEqual(self.EXPECTED_HEADERS, kw_args['headers'])
        actual_data = kw_args['data']
        actual_data.seek(0)
        self.assertEqual(self.PARAMS_JSON_BYTES, actual_data.read())

    def test_get_content(self):
        self.requests.get.return_value = self.response
        self.response.status_code = 200
        with self.b2_http.get_content(self.URL, self.HEADERS) as r:
            if IS_27_OR_LATER:
                self.assertIs(self.response, r)
        self.requests.get.assert_called_with(self.URL, headers=self.EXPECTED_HEADERS)
        self.response.close.assert_called_with()
