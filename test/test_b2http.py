######################################################################
#
# File: test/test_b2http.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import datetime
import requests
import six
import socket
import sys

from .test_base import TestBase
from b2.b2http import _translate_and_retry, _translate_errors, B2Http, ClockSkewHook
from b2.exception import BadDateFormat, BadJson, BrokenPipe, B2ConnectionError, ClockSkew, ConnectionReset, ServiceError, UnknownError, UnknownHost
from b2.version import USER_AGENT

if sys.version_info < (3, 3):
    from mock import call, MagicMock, patch
else:
    from unittest.mock import call, MagicMock, patch


class TestTranslateErrors(TestBase):
    def test_ok(self):
        response = MagicMock()
        response.status_code = 200
        actual = _translate_errors(lambda: response)
        self.assertTrue(response is actual)  # no assertIs until 2.7

    def test_partial_content(self):
        response = MagicMock()
        response.status_code = 206
        actual = _translate_errors(lambda: response)
        self.assertTrue(response is actual)  # no assertIs until 2.7

    def test_b2_error(self):
        response = MagicMock()
        response.status_code = 503
        response.content = six.b('{"status": 503, "code": "server_busy", "message": "busy"}')
        with self.assertRaises(ServiceError):
            _translate_errors(lambda: response)

    def test_broken_pipe(self):
        def fcn():
            raise requests.ConnectionError(
                requests.packages.urllib3.exceptions.ProtocolError(
                    "dummy", socket.error(20, 'Broken pipe')
                )
            )

        with self.assertRaises(BrokenPipe):
            _translate_errors(fcn)

    def test_unknown_host(self):
        def fcn():
            raise requests.ConnectionError(
                requests.packages.urllib3.exceptions.MaxRetryError(
                    'AAA nodename nor servname provided, or not known AAA', 'http://example.com'
                )
            )

        with self.assertRaises(UnknownHost):
            _translate_errors(fcn)

    def test_connection_error(self):
        def fcn():
            raise requests.ConnectionError('a message')

        with self.assertRaises(B2ConnectionError):
            _translate_errors(fcn)

    def test_connection_reset(self):
        class SysCallError(Exception):
            pass

        def fcn():
            raise SysCallError('(104, ECONNRESET)')

        with self.assertRaises(ConnectionReset):
            _translate_errors(fcn)

    def test_unknown_error(self):
        def fcn():
            raise Exception('a message')

        with self.assertRaises(UnknownError):
            _translate_errors(fcn)


class TestTranslateAndRetry(TestBase):
    def setUp(self):
        self.response = MagicMock()
        self.response.status_code = 200

    def test_works_first_try(self):
        fcn = MagicMock()
        fcn.side_effect = [self.response]
        self.assertTrue(self.response is _translate_and_retry(fcn, 3))  # no assertIs until 2.7

    def test_non_retryable(self):
        with patch('time.sleep') as mock_time:
            fcn = MagicMock()
            fcn.side_effect = [BadJson('a'), self.response]
            # no assertRaises until 2.7
            try:
                _translate_and_retry(fcn, 3)
                self.fail('should have raised BadJson')
            except BadJson:
                pass
            self.assertEqual([], mock_time.mock_calls)

    def test_works_second_try(self):
        with patch('time.sleep') as mock_time:
            fcn = MagicMock()
            fcn.side_effect = [ServiceError('a'), self.response]
            self.assertTrue(self.response is _translate_and_retry(fcn, 3))  # no assertIs until 2.7
            self.assertEqual([call(1.0)], mock_time.mock_calls)

    def test_never_works(self):
        with patch('time.sleep') as mock_time:
            fcn = MagicMock()
            fcn.side_effect = [
                ServiceError('a'),
                ServiceError('a'),
                ServiceError('a'), self.response
            ]
            # no assertRaises until 2.7
            try:
                _translate_and_retry(fcn, 3)
                self.fail('should have raised ServiceError')
            except ServiceError:
                pass
            self.assertEqual([call(1.0), call(1.5)], mock_time.mock_calls)


class TestB2Http(TestBase):

    URL = 'http://example.com'
    HEADERS = dict(my_header='my_value')
    EXPECTED_HEADERS = {'my_header': 'my_value', 'User-Agent': USER_AGENT}
    PARAMS = dict(fileSize=100)
    PARAMS_JSON_BYTES = six.b('{"fileSize": 100}')

    def setUp(self):
        self.session = MagicMock()
        self.response = MagicMock()

        requests = MagicMock()
        requests.Session.return_value = self.session
        self.b2_http = B2Http(requests, install_clock_skew_hook=False)

    def test_post_json_return_json(self):
        self.session.post.return_value = self.response
        self.response.status_code = 200
        self.response.content = six.b('{"color": "blue"}')
        response_dict = self.b2_http.post_json_return_json(self.URL, self.HEADERS, self.PARAMS)
        self.assertEqual({'color': 'blue'}, response_dict)
        (pos_args, kw_args) = self.session.post.call_args
        self.assertEqual(self.URL, pos_args[0])
        self.assertEqual(self.EXPECTED_HEADERS, kw_args['headers'])
        actual_data = kw_args['data']
        actual_data.seek(0)
        self.assertEqual(self.PARAMS_JSON_BYTES, actual_data.read())

    def test_callback(self):
        callback = MagicMock()
        callback.pre_request = MagicMock()
        callback.post_request = MagicMock()
        self.b2_http.add_callback(callback)
        self.session.post.return_value = self.response
        self.response.status_code = 200
        self.response.content = six.b('{"color": "blue"}')
        self.b2_http.post_json_return_json(self.URL, self.HEADERS, self.PARAMS)
        expected_headers = {'my_header': 'my_value', 'User-Agent': USER_AGENT}
        callback.pre_request.assert_called_with('POST', 'http://example.com', expected_headers)
        callback.post_request.assert_called_with(
            'POST', 'http://example.com', expected_headers, self.response
        )

    def test_get_content(self):
        self.session.get.return_value = self.response
        self.response.status_code = 200
        with self.b2_http.get_content(self.URL, self.HEADERS) as r:
            self.assertTrue(self.response is r)  # no assertIs until 2.7
        self.session.get.assert_called_with(self.URL, headers=self.EXPECTED_HEADERS, stream=True)
        self.response.close.assert_called_with()


class TestClockSkewHook(TestBase):
    def test_bad_format(self):
        response = MagicMock()
        response.headers = {'Date': 'bad format'}
        with self.assertRaises(BadDateFormat):
            ClockSkewHook().post_request('POST', 'http://example.com', {}, response)

    def test_bad_month(self):
        response = MagicMock()
        response.headers = {'Date': 'Fri, 16 XXX 2016 20:52:30 GMT'}
        with self.assertRaises(BadDateFormat):
            ClockSkewHook().post_request('POST', 'http://example.com', {}, response)

    def test_no_skew(self):
        now = datetime.datetime.utcnow()
        now_str = now.strftime('%a, %d %b %Y %H:%M:%S GMT')
        response = MagicMock()
        response.headers = {'Date': now_str}
        ClockSkewHook().post_request('POST', 'http://example.com', {}, response)

    def test_positive_skew(self):
        now = datetime.datetime.utcnow() + datetime.timedelta(minutes=11)
        now_str = now.strftime('%a, %d %b %Y %H:%M:%S GMT')
        response = MagicMock()
        response.headers = {'Date': now_str}
        with self.assertRaises(ClockSkew):
            ClockSkewHook().post_request('POST', 'http://example.com', {}, response)

    def test_negative_skew(self):
        now = datetime.datetime.utcnow() + datetime.timedelta(minutes=-11)
        now_str = now.strftime('%a, %d %b %Y %H:%M:%S GMT')
        response = MagicMock()
        response.headers = {'Date': now_str}
        with self.assertRaises(ClockSkew):
            ClockSkewHook().post_request('POST', 'http://example.com', {}, response)
