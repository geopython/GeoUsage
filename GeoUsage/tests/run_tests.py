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
import unittest

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch


from GeoUsage.log import Analyzer, NotFoundError, OWSLogRecord, WMSLogRecord
from GeoUsage.mailing_list import MailmanAdmin

THISDIR = os.path.dirname(os.path.realpath(__file__))


class LogTest(unittest.TestCase):
    """Test case for package GeoUsage.log.WMSLogRecord"""

    def test_log(self):
        """test log functionality"""

        records = []

        access_log = get_abspath('access.log')

        with open(access_log) as ff:
            for line in ff.readlines():
                try:
                    lr = WMSLogRecord(line)
                    records.append(lr)
                except NotFoundError:
                    pass

        self.assertEqual(len(records), 340)

        single_record = records[4]
        self.assertEqual(single_record._line, '142.97.203.36 - - [26/Jan/2018:13:13:22 +0000] "GET /geomet?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetCapabilities HTTP/1.1" 200 7176380 "-" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET4.0C; .NET4.0E; .NET CLR 3.5.30729; .NET CLR 3.0.30729; InfoPath.3)"')  # noqa
        self.assertEqual(single_record.remote_host, '142.97.203.36')
        self.assertEqual(single_record.datetime,
                         datetime(2018, 1, 26, 13, 13, 22))
        self.assertEqual(single_record.timezone, '+0000')
        self.assertEqual(single_record.request_type, 'GET')
        self.assertEqual(single_record.request,
                         '/geomet?SERVICE=WMS&VERSION=1.1.1&REQUEST=GetCapabilities')  # noqa
        self.assertEqual(single_record.protocol, 'HTTP/1.1')
        self.assertEqual(single_record.status_code, 200)
        self.assertEqual(single_record.size, 7176380)
        self.assertEqual(single_record.referer, '-')
        self.assertEqual(single_record.user_agent, 'Mozilla/4.0')

        self.assertEqual(single_record.baseurl, '/geomet')
        self.assertEqual(single_record.service, 'WMS')
        self.assertEqual(single_record.version, '1.1.1')
        self.assertEqual(single_record.ows_request, 'GetCapabilities')
        self.assertEqual(len(single_record.kvp), 3)
        self.assertEqual(single_record.resource, 'layers')


class AnalyzerTest(unittest.TestCase):
    """Test case for GeoUsage.log.Analyzer"""

    def test_analyzer(self):
        """test log analysis functionality"""

        records = []

        access_log = get_abspath('access.log')

        with open(access_log) as ff:
            for line in ff.readlines():
                try:
                    lr = WMSLogRecord(line)
                    records.append(lr)
                except NotFoundError:
                    pass

        self.assertEqual(len(records), 340)

        a = Analyzer(records)

        self.assertEqual(a.start, datetime(2018, 1, 26, 11, 44, 25))
        self.assertEqual(a.end, datetime(2018, 1, 26, 22, 42, 12))
        self.assertEqual(a.total_requests, 339)
        self.assertEqual(a.total_size, 882128934)
        self.assertEqual(len(a.unique_ips), 8)

        with self.assertRaises(NotFoundError):
            with open(access_log) as ff:
                for line in ff.readlines():
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
    unittest.main()
