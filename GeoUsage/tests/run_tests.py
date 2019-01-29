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
from elasticsearch5 import Elasticsearch
import gzip
import re
import os
import time
import math
import unittest
from unittest.runner import TextTestResult
from unittest import TextTestRunner

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch


from GeoUsage.log import (Analyzer, NotFoundError, LogRecord, OWSLogRecord, WMSLogRecord,
                          parse_iso8601, test_time)
from GeoUsage.log_es import (indexRecord, locateIP, parse_ip2location, download_ip2location, unzip_ip2location,
                            LOG_SETTINGS_MAPPINGS, IP2LOCATION_PATH, IP2LOCATION_FILE_NAME,
                            log_to_index, create_index, delete_index, get_server_source)
from GeoUsage.mailing_list import MailmanAdmin

THISDIR = os.path.dirname(os.path.realpath(__file__))

SLOW_TEST_THRESHOLD = 0.3

class TimeLoggingTestResult(TextTestResult):
    """Outputs timing of slow running tests. Slowness threshold is the number of seconds defined by SLOW_TEST_THRESHOLD"""

    def startTest(self, test):
        self._started_at = time.time()
        super().startTest(test)

    def addSuccess(self, test):
        elapsed = time.time() - self._started_at
        m, s = divmod(elapsed, 60)
        h, m = divmod(m, 60)
        if elapsed > SLOW_TEST_THRESHOLD:
            name = self.getDescription(test)
            self.stream.write("\n{} ({}h {}m {:.03}s)\n".format(name, h, m, s))
        super().addSuccess(test)

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
        self.assertEqual(single_record.user_agent, 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:57.0) Gecko/20100101 Firefox/57.0')

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

class ElasticSearchTest(unittest.TestCase):
    """Test case for parsing logs and inserting to ES"""

    LARGE_RECORDS = []
    IP2LOCATION = []
    server_source = None

    def setUp(self):
        pass

    def test_c_download_ip2location(self):
        """Download, unzip and extract the IP2LOCATION csv file to disk"""

        filename = IP2LOCATION_FILE_NAME
        zip_contents = download_ip2location()
        extracted_filename = unzip_ip2location(zip_contents)

        self.assertEqual(filename, extracted_filename)

    def test_a_parse_ip2location(self):
        """Parses the IP2LOCATION csv file for IP location lookup"""

        ip2location_csv = IP2LOCATION_PATH

        self.assertTrue(os.path.isfile(ip2location_csv))

        self.__class__.IP2LOCATION = parse_ip2location(ip2location_csv)
        
        self.assertTrue(len(self.__class__.IP2LOCATION) > 1000000) # roughly 2.8m records

    def test_a_parse_large_log(self):
        """Test case for parsing a very large gzipped log file using OWSLogRecord"""

        access_log = '/path/to/logs-server-0N/access_server_num_0N_log-2019MMDDTime.gz'
        self.__class__.server_source = get_server_source(access_log)

        if access_log.endswith('gz'):
            open_ = gzip.open
        else:
            open_ = open

        with open_(access_log, 'rt') as ff:
            for line in ff:
                try:
                    lr = OWSLogRecord(line)
                    self.__class__.LARGE_RECORDS.append(lr)
                except NotFoundError:
                    pass

                if len(self.__class__.LARGE_RECORDS) >= 50000:
                    break # TEMPORARY break to limit records

        self.assertTrue(len(self.__class__.LARGE_RECORDS) > 1)

    def test_b_create_index_delete_large_log(self):
        """Test case for bulk insert large records to an ES index"""

        es = Elasticsearch()

        index_name = 'log_large_test'
        delete_existing_es = True
        bulk_size = 5000
        refresh_index = True
        create_id = True
        server_source = self.__class__.server_source

        res = log_to_index(self.__class__.LARGE_RECORDS, LOG_SETTINGS_MAPPINGS, index_name, self.__class__.IP2LOCATION, delete_existing_es, bulk_size, refresh_index, create_id, server_source)

        # sanity check
        query_search = {
            'query': {
                'match': {
                    # 'properties.remote_host_ip': '35.183.207.115'
                    # 'remote_host_ip': '54.155.20.100'
                    'properties.status_code': 200
                }
            },
            'sort': [
                {'properties.datetime': 'desc'}
            ]
        }
        res = es.search(index=index_name, size=2, body=query_search)

        self.maxDiff = None

        self.assertEqual(res['hits']['hits'][0]['_source']['properties']['protocol'], 'HTTP/1.1')
        self.assertTrue(len(res['hits']['hits']) > 0)

    def test_es_index_bad_log(self):
        """Test case for indexing a bad log file"""

        access_log = get_abspath('bad_access.log')
        records = []

        with open(access_log, 'rt') as ff:
            for line in ff:
                try:
                    lr = OWSLogRecord(line)
                    records.append(lr)
                except NotFoundError:
                    pass

        self.assertEqual(len(records), 5)

        es = Elasticsearch()

        index_name = 'log_bad_records_test'
        delete_existing_es = True
        bulk_size = 5000
        refresh_index = True,
        create_id = True
        server_source = 'bad_access.log'

        res = log_to_index(records, LOG_SETTINGS_MAPPINGS, index_name, self.__class__.IP2LOCATION, delete_existing_es, bulk_size, refresh_index, create_id, server_source)

        # sanity check
        query_search = {
            'query': {
                'match': {
                    'properties.status_code': 200
                }
            },
            'sort': [
                {'properties.datetime': 'desc'}
            ]
        }
        res = es.search(index=index_name, size=2, body=query_search)

        self.maxDiff = None

        self.assertTrue(len(res['hits']['hits']) > 0)


    def test_es_create_index_delete_txt_file(self):
        """Test case for parsing a small static text log file and inserting to ES index line by line"""

        access_log = get_abspath('access.log')

        es = Elasticsearch()

        index_name = 'log_test'
        self.assertEqual(index_name, 'log_test')

        # delete existing index if exists
        if es.indices.exists(index_name):
            res = es.indices.delete(index=index_name)
            self.assertTrue(res['acknowledged'])

        # create index
        res = create_index(index_name, LOG_SETTINGS_MAPPINGS)
        self.assertTrue(res['shards_acknowledged'])
        self.assertEqual(res['index'], index_name)

        # insert document one by one
        with open(access_log, 'rt') as ff:
            for line in ff:
                try:
                    index_record = indexRecord(line, self.__class__.IP2LOCATION)

                    es.index(index=index_name, doc_type='FeatureCollection', body=index_record)
                except NotFoundError:
                    pass

        self.assertTrue(es.indices.exists(index_name))

        # sanity check
        query_search = {
            'query': {
                'match': {
                    'properties.remote_host_ip': '142.97.203.36'
                }
            },
            'sort': [
                {'properties.datetime': 'desc'}
            ]
        }
        res = es.search(index=index_name, size=2, body=query_search)

        self.assertEqual(res['hits']['hits'][0]['_source']['properties']['request'], '/geomet?SERVICE=WMS&VERSION=1.3.0&REQUEST=GetMap&BBOX=-90,-180,90,180&CRS=EPSG:4326&WIDTH=638&HEIGHT=320&LAYERS=RDPS.ETA_PN&STYLES=&FORMAT=image/png&DPI=96&MAP_RESOLUTION=96&FORMAT_OPTIONS=dpi:96&TRANSPARENT=TRUE')  # noqa
        self.assertEqual(res['hits']['hits'][0]['_source']['properties']['protocol'], 'HTTP/1.1')
        self.assertEqual(res['hits']['hits'][0]['_source']['properties']['remote_host_ip'], '142.97.203.36')
        self.assertEqual(res['hits']['hits'][0]['_source']['properties']['status_code'], 200)
        self.assertEqual(res['hits']['hits'][0]['_source']['properties']['size'], 25234)
        self.assertEqual(res['hits']['hits'][0]['_source']['properties']['referer'], '-')
        self.assertEqual(res['hits']['hits'][0]['_source']['properties']['user_agent'], 'Mozilla/5.0 QGIS/2.14.8-Essen')
        # self.assertEqual(res['hits']['hits'][0]['_source']['properties']['kvp']['layers'], 'RDPS.ETA_PN')

def get_abspath(filepath):
    """helper function absolute file access"""

    return os.path.join(THISDIR, filepath)

if __name__ == '__main__':
    # Show elapsed time for slow test cases
    test_runner = TextTestRunner(resultclass=TimeLoggingTestResult)
    unittest.main(verbosity=2, testRunner=test_runner)
