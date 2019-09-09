###############################################################################
#
# The MIT License (MIT)
# Copyright (c) 2018 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
# DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
# USE OR OTHER DEALINGS IN THE SOFTWARE.
#
###############################################################################

from datetime import datetime
import os
import time
import unittest
from unittest.runner import TextTestResult
from unittest import TextTestRunner

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch


from GeoUsage.log import (Analyzer, NotFoundError, OWSLogRecord, WMSLogRecord,
                          parse_iso8601, test_time, dot2longip)
from GeoUsage.mailing_list import MailmanAdmin

THISDIR = os.path.dirname(os.path.realpath(__file__))

SLOW_TEST_THRESHOLD = 0.3


class TimeLoggingTestResult(TextTestResult):
    """Outputs timing of slow running tests. Slowness threshold is the number
    of seconds defined by SLOW_TEST_THRESHOLD"""

    def start_test(self, test):
        self._started_at = time.time()
        super().start_test(test)

    def add_success(self, test):
        elapsed = time.time() - self._started_at
        m, s = divmod(elapsed, 60)
        h, m = divmod(m, 60)
        if elapsed > SLOW_TEST_THRESHOLD:
            name = self.getDescription(test)
            self.stream.write("\n{} ({}h {}m {:.03}s)\n".format(name, h, m, s))
        super().add_success(test)


class LogTest(unittest.TestCase):
    """Test case for package GeoUsage.log.WMSLogRecord"""

    def test_log(self):
        """test log functionality"""

        records = []

        access_log = get_abspath('access.log')

        with open(access_log, 'rt') as ff:
            for line in ff:
                try:
                    lr = WMSLogRecord(line)
                    records.append(lr)
                except NotFoundError:
                    pass

        self.assertEqual(len(records), 341)

        single_record = records[4]
        self.assertEqual(single_record._line, '131.235.251.154 - - [23/Jan/2018:13:09:45 +0000] "GET /geomet/?service=WMS&version=1.3.0&request=GetCapabilities HTTP/1.1" 200 101395 "-" "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:57.0) Gecko/20100101 Firefox/57.0"')  # noqa
        self.assertEqual(single_record.remote_host_ip, '131.235.251.154')
        self.assertEqual(single_record.datetime,
                         datetime(2018, 1, 23, 13, 9, 45))
        self.assertEqual(single_record.timezone, '+0000')
        self.assertEqual(single_record.request_type, 'GET')
        self.assertEqual(single_record.request,
                         '/geomet/?service=WMS&version=1.3.0&request=GetCapabilities')  # noqa
        self.assertEqual(single_record.protocol, 'HTTP/1.1')
        self.assertEqual(single_record.status_code, 200)
        self.assertEqual(single_record.size, 101395)
        self.assertEqual(single_record.referer, '-')
        self.assertEqual(single_record.user_agent, ('Mozilla/5.0 (Windows NT '
                         '6.1; WOW64; rv:57.0) Gecko/20100101 Firefox/57.0'))
        self.assertEqual(single_record.baseurl, '/geomet/')
        self.assertEqual(single_record.service, 'WMS')
        self.assertEqual(single_record.version, '1.3.0')
        self.assertEqual(single_record.ows_request, 'GetCapabilities')
        self.assertEqual(len(single_record.kvp), 3)
        self.assertEqual(single_record.resource, 'layers')

        # constrain to a specific endpoint
        records = []
        with open(access_log) as ff:
            for line in ff:
                try:
                    lr = WMSLogRecord(line, endpoint='/geomet2')
                    records.append(lr)
                except NotFoundError:
                    pass

        self.assertEqual(len(records), 0)

    def test_parse_iso8601(self):
        """test GeoUsage.log.parse_iso8601"""

        val = '2011-11-11'
        result = parse_iso8601(val)
        self.assertEqual(result, [datetime(2011, 11, 11, 0, 0)])

        val = '2011-11-11T11:11:11'
        result = parse_iso8601(val)
        self.assertEqual(result, [datetime(2011, 11, 11, 11, 11, 11)])

        val = '2011-11-11/2012-11-23'
        result = parse_iso8601(val)
        self.assertEqual(result, [datetime(2011, 11, 11, 0, 0),
                                  datetime(2012, 11, 23, 0, 0)])

    def test_test_time(self):
        """test GeoUsage.log.test_test_time"""

        indate = datetime(2011, 11, 11, 0, 0)

        # test single date
        dates = [datetime(2011, 11, 11, 0, 0)]
        result = test_time(indate, dates, datetype='date')
        self.assertTrue(result)

        dates = [datetime(2011, 12, 11, 0, 0)]
        result = test_time(indate, dates, datetype='date')
        self.assertFalse(result)

        # test date range
        dates = [datetime(2010, 12, 11, 0, 0), datetime(2012, 12, 11, 0, 0)]
        result = test_time(indate, dates, datetype='date')
        self.assertTrue(result)

        dates = [datetime(2012, 12, 11, 0, 0), datetime(2013, 12, 11, 0, 0)]
        result = test_time(indate, dates, datetype='date')
        self.assertFalse(result)

        intime = datetime(2011, 11, 11, 11, 11, 11)

        # test single datetime
        times = [datetime(2011, 11, 11, 11, 11, 11)]
        result = test_time(intime, times, datetype='datetime')
        self.assertTrue(result)

        times = [datetime(2011, 12, 11, 0, 0)]
        result = test_time(intime, times, datetype='datetime')
        self.assertFalse(result)

        # test datetime range
        times = [datetime(2010, 12, 11, 11, 11, 11),
                 datetime(2012, 12, 11, 11, 12, 42)]
        result = test_time(intime, times, datetype='datetime')
        self.assertTrue(result)

        times = [datetime(2012, 12, 11, 22, 22, 22),
                 datetime(2013, 12, 11, 14, 55, 22)]
        result = test_time(intime, times, datetype='datetime')
        self.assertFalse(result)

    def test_dot2longip(self):
        """Test function that converts an IP address to an IP number"""

        ip = '172.217.15.110'
        ip_number = dot2longip(ip)
        self.assertEqual(ip_number, 2899906414)

        ip = '23.78.198.125'
        ip_number = dot2longip(ip)
        self.assertEqual(ip_number, 391038589)

        ip = ''
        ip_number = dot2longip(ip)
        self.assertEqual(ip_number, 0)

        ip = None
        ip_number = dot2longip(ip)
        self.assertEqual(ip_number, 0)


class AnalyzerTest(unittest.TestCase):
    """Test case for GeoUsage.log.Analyzer"""

    def test_analyzer(self):
        """test log analysis functionality"""

        records = []

        access_log = get_abspath('access.log')

        with open(access_log) as ff:
            for line in ff:
                try:
                    lr = WMSLogRecord(line)
                    records.append(lr)
                except NotFoundError:
                    pass

        self.assertEqual(len(records), 341)

        a = Analyzer(records)

        self.assertEqual(a.start, datetime(2018, 1, 23, 11, 42, 25))
        self.assertEqual(a.end, datetime(2018, 1, 28, 22, 42, 12))
        self.assertEqual(a.total_requests, 339)
        self.assertEqual(a.total_size, 882128944)
        self.assertEqual(len(a.unique_ips), 8)

        unique_ips = dict(a.unique_ips)
        self.assertTrue('131.235.251.154' in unique_ips)
        self.assertTrue(unique_ips['131.235.251.154']['count'], 24)

        a = Analyzer([])
        self.assertEqual(a.start, None)
        self.assertEqual(a.end, None)
        self.assertEqual(a.requests, {})

        with self.assertRaises(NotFoundError):
            with open(access_log) as ff:
                for line in ff:
                    lr = OWSLogRecord(line, service_type='OGC:WFS')


class MailmanAdminTest(unittest.TestCase):
    """Test case for GeoUsage.mailing_list.MailmanAdmin"""

    @patch('requests.post')
    def test_member_count(self, mock_get):
        """test mailing list member count"""

        mock_get.return_value.ok = True
        mock_get.return_value.text = '18 members total'

        ma = MailmanAdmin('http://example.org/mailmain/admin/list', 'secret')

        self.assertEqual(ma.member_count, 18)


def get_abspath(filepath):
    """helper function absolute file access"""

    return os.path.join(THISDIR, filepath)


if __name__ == '__main__':
    # Show elapsed time for slow test cases
    test_runner = TextTestRunner(resultclass=TimeLoggingTestResult)
    unittest.main(verbosity=2, testRunner=test_runner)
