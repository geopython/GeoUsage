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
from datetime import timedelta
from elasticsearch5 import Elasticsearch
import glob
import hashlib
import gzip
import logging
import socket
import csv
import math
from io import BytesIO
from zipfile import ZipFile
import requests
import os
import click

from GeoUsage.log import OWSLogRecord, NotFoundError

LOGGER = logging.getLogger(__name__)

# env variables
IP2LOCATION_TOKEN = 'JD7X0V79LcsfoQgoslnxCWmaVugN2CiLfqJgmQq83TnrJEQcZdJCa5M5A2TCEcoU'
IP2LOCATION_EXTRACT_PATH = '/data/geomet/ip2location'
IP2LOCATION_FILE_NAME = 'IP2LOCATION-LITE-DB5.CSV'
ES_INDEX_NAME_LOGS_ALL = 'log_geomet_access'
ES_INDEX_NAME_LOGS_DAILY = 'log_geomet_access_daily_stats'

IP2LOCATION_PATH = IP2LOCATION_EXTRACT_PATH + '/' + IP2LOCATION_FILE_NAME

# strict mapping for apache log records
LOG_SETTINGS_MAPPINGS = {
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0
    },
    'mappings': {
        # GeoJSON formatting
        'FeatureCollection': {
            'dynamic': 'strict',
            '_meta': {
                'geomfields': {
                    'geometry': 'POINT'
                }
            },
            'properties': {
                'type': {'type': 'text'},
                'geometry': {
                    'type': 'geo_shape',
                    'points_only': 'true'
                },
                'properties': {
                    'type': 'object', 
                    'dynamic': 'strict',
                    'properties': {
                        'remote_host_ip': {'type': 'ip'},
                        'ip_number': {'type': 'long'},
                        'city': {
                            'type': 'text', 
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'country': {
                            'type': 'text', 
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'country_code': {
                            'type': 'keyword'
                        },
                        'province': {
                            'type': 'text', 
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'datetime': {'type': 'date', 'format': 'strict_date_hour_minute_second'},
                        'timezone': {
                            'type': 'text', 
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'request_type': {
                            'type': 'text', 
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'request': {
                            'type': 'text', 
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'protocol': {
                            'type': 'text', 
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'status_code': {'type': 'integer'},
                        'size': {'type': 'integer'},
                        'referer': {
                            'type': 'text', 
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'user_agent': {
                            'type': 'text', 
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'baseurl': {
                            'type': 'text', 
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'service': {
                            'type': 'keyword'
                        },
                        'version': {
                            'type': 'text', 
                            "fields": {
                                "raw": {
                                    "type": "keyword"
                                }
                            }
                        },
                        'ows_request': {
                            'type': 'keyword'
                        },
                        'crs': {
                            'type': 'keyword'
                        },
                        'format': {
                            'type': 'keyword'
                        },
                        'styles': {
                            'type': 'keyword'
                        },
                        'kvp': {
                            'type': 'object', 
                            'dynamic': 'true',
                            'properties': {
                                'time': {'type': 'text'},
                                'layers': {'type': 'keyword'}
                            }
                        },
                        'ows_resource': {
                            'type': 'text', 
                            "fields": {
                              "raw": {
                                "type": "keyword"
                              }
                            }
                        },
                        'server_source': {
                            'type': 'keyword'
                        }
                    }
                }
            }
        }
    }
}

# mapping for daily rolled up apache log records
LOG_DAILY_SETTINGS_MAPPINGS = {
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0
    },
    'mappings': {
        'stats': {
            'dynamic': 'true',
            'properties': {
                'date': {'type': 'date', 'format': 'strict_date'},
                'date_min': {'type': 'date', 'format': 'strict_date_hour_minute_second'},
                'date_max': {'type': 'date', 'format': 'strict_date_hour_minute_second'},
                'total_requests': {'type': 'long'},
                'total_size': {'type': 'long'},
                'unique_ips': {
                    'properties': {
                        'ip': {'type': 'keyword'},
                        'count': {'type': 'integer'},
                        'hostname': {'type': 'keyword'}
                    }
                },
                'resources': {
                    'properties': {
                        'layers': {'type': 'keyword'},
                        'count': {'type': 'integer'}
                    }
                },
                'requests': {
                    'properties': {
                        'request': {'type': 'keyword'},
                        'count': {'type': 'integer'}
                    }
                },
                'user_agents': {
                    'properties': {
                        'agent': {'type': 'text'},
                        'count': {'type': 'integer'}
                    }
                }
            }
        }
    }
}

# mapping for daily aggregated stats of log records
LOG_DAILY_AGG_SETTINGS_MAPPINGS = {
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0
    },
    'mappings': {
        'stats': {
            'dynamic': 'false',
            'properties': {
                'date': {'type': 'date', 'format': 'strict_date'},
                'date_min': {'type': 'date', 'format': 'strict_date_hour_minute_second'},
                'date_max': {'type': 'date', 'format': 'strict_date_hour_minute_second'},
                'total_requests': {'type': 'long'},
                'total_size': {'type': 'long'},
                'size_percentiles': {
                    'properties': {
                        'key': {'type': 'float'},
                        'value': {'type': 'long'}
                    }
                },
                'datetime_percentiles': {
                    'properties': {
                        'key': {'type': 'float'},
                        'value': {'type': 'long'},
                        'value_as_string': {'type': 'text'}
                    }
                },
                'size_stats': {
                    'type': 'object',
                    'dynamic': 'false',
                    'properties': {
                        'min': {'type': 'integer'},
                        'max': {'type': 'integer'},
                        'avg': {'type': 'double'},
                        'sum': {'type': 'long'}
                    }
                },
                'datetime_stats': {
                    'type': 'object',
                    'dynamic': 'false',
                    'properties': {
                        'min': {'type': 'long'},
                        'max': {'type': 'long'},
                        'avg': {'type': 'double'},
                        'min_as_string': {'type': 'text'},
                        'max_as_string': {'type': 'text'},
                        'avg_as_string': {'type': 'text'}
                    }
                },
                'top_50_user_agents': {
                    'type': 'object',
                    'properties': {
                        'doc_count_error_upper_bound': {'type': 'integer'},
                        'sum_other_doc_count': {'type': 'integer'},
                        'buckets': {
                            'properties': {
                                'key': {'type': 'text'},
                                'doc_count': {'type': 'integer'}
                            }
                        }
                    }
                },
                'top_10_crs': {
                    'type': 'object',
                    'properties': {
                        'doc_count_error_upper_bound': {'type': 'integer'},
                        'sum_other_doc_count': {'type': 'integer'},
                        'buckets': {
                            'properties': {
                                'key': {'type': 'text'},
                                'doc_count': {'type': 'integer'}
                            }
                        }
                    }
                },
                'top_10_formats': {
                    'type': 'object',
                    'properties': {
                        'doc_count_error_upper_bound': {'type': 'integer'},
                        'sum_other_doc_count': {'type': 'integer'},
                        'buckets': {
                            'properties': {
                                'key': {'type': 'text'},
                                'doc_count': {'type': 'integer'}
                            }
                        }
                    }
                },
                'top_10_ows_request_types': {
                    'type': 'object',
                    'properties': {
                        'doc_count_error_upper_bound': {'type': 'integer'},
                        'sum_other_doc_count': {'type': 'integer'},
                        'buckets': {
                            'properties': {
                                'key': {'type': 'text'},
                                'doc_count': {'type': 'integer'}
                            }
                        }
                    }
                },
                'top_100_ows_resources': {
                    'type': 'object',
                    'properties': {
                        'doc_count_error_upper_bound': {'type': 'integer'},
                        'sum_other_doc_count': {'type': 'integer'},
                        'buckets': {
                            'properties': {
                                'key': {'type': 'text'},
                                'doc_count': {'type': 'integer'}
                            }
                        }
                    }
                },
                'top_50_countries': {
                    'type': 'object',
                    'properties': {
                        'doc_count_error_upper_bound': {'type': 'integer'},
                        'sum_other_doc_count': {'type': 'integer'},
                        'buckets': {
                            'properties': {
                                'key': {'type': 'text'},
                                'doc_count': {'type': 'integer'}
                            }
                        }
                    }
                },
                'top_100_cities': {
                    'type': 'object',
                    'properties': {
                        'doc_count_error_upper_bound': {'type': 'integer'},
                        'sum_other_doc_count': {'type': 'integer'},
                        'buckets': {
                            'properties': {
                                'key': {'type': 'text'},
                                'doc_count': {'type': 'integer'}
                            }
                        }
                    }
                },
                'top_50_unique_ips_with_details': {
                    'type': 'object',
                    'properties': {
                        'doc_count_error_upper_bound': {'type': 'integer'},
                        'sum_other_doc_count': {'type': 'integer'},
                        'buckets': {
                            'properties': {
                                'key': {'type': 'text'},
                                'doc_count': {'type': 'integer'},
                                'country': {
                                    'type': 'object',
                                    'properties': {
                                        'doc_count_error_upper_bound': {'type': 'integer'},
                                        'sum_other_doc_count': {'type': 'integer'},
                                        'buckets': {
                                            'properties': {
                                                'key': {'type': 'text'},
                                                'doc_count': {'type': 'integer'}
                                            }
                                        }
                                    }
                                },
                                'size_stats': {
                                    'type': 'object',
                                    'dynamic': 'false',
                                    'properties': {
                                        'min': {'type': 'integer'},
                                        'max': {'type': 'integer'},
                                        'avg': {'type': 'double'},
                                        'sum': {'type': 'long'}
                                    }
                                },
                                "top_10_ows_resources": {
                                    'type': 'object',
                                    'properties': {
                                        'doc_count_error_upper_bound': {'type': 'integer'},
                                        'sum_other_doc_count': {'type': 'integer'},
                                        'buckets': {
                                            'properties': {
                                                'key': {'type': 'text'},
                                                'doc_count': {'type': 'integer'}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                'top_10_services_with_details': {
                    'type': 'object',
                    'properties': {
                        'doc_count_error_upper_bound': {'type': 'integer'},
                        'sum_other_doc_count': {'type': 'integer'},
                        'buckets': {
                            'properties': {
                                'key': {'type': 'text'},
                                'doc_count': {'type': 'integer'},
                                'formats': {
                                    'type': 'object',
                                    'properties': {
                                        'doc_count_error_upper_bound': {'type': 'integer'},
                                        'sum_other_doc_count': {'type': 'integer'},
                                        'buckets': {
                                            'properties': {
                                                'key': {'type': 'text'},
                                                'doc_count': {'type': 'integer'}
                                            }
                                        }
                                    }
                                },
                                'versions': {
                                    'type': 'object',
                                    'properties': {
                                        'doc_count_error_upper_bound': {'type': 'integer'},
                                        'sum_other_doc_count': {'type': 'integer'},
                                        'buckets': {
                                            'properties': {
                                                'key': {'type': 'text'},
                                                'doc_count': {'type': 'integer'}
                                            }
                                        }
                                    }
                                },
                                'crs': {
                                    'type': 'object',
                                    'properties': {
                                        'doc_count_error_upper_bound': {'type': 'integer'},
                                        'sum_other_doc_count': {'type': 'integer'},
                                        'buckets': {
                                            'properties': {
                                                'key': {'type': 'text'},
                                                'doc_count': {'type': 'integer'}
                                            }
                                        }
                                    }
                                },
                                'ows_resource': {
                                    'type': 'object',
                                    'properties': {
                                        'doc_count_error_upper_bound': {'type': 'integer'},
                                        'sum_other_doc_count': {'type': 'integer'},
                                        'buckets': {
                                            'properties': {
                                                'key': {'type': 'text'},
                                                'doc_count': {'type': 'integer'}
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

ES_QUERY_DAILY_LOGS = {
    "size": 0,
    "query": {
        "range": {
            "properties.datetime": {
                "gte": None,
                "lt": None
            }
        }   
    },
    "aggs" : {
        "unique_ips_count" : {
            "cardinality" : { "field" : "properties.remote_host_ip" }
        },
        "top_10_ows_request_types" : {
            "terms" : { "field" : "properties.ows_request", "size": 10 }
        },
        "size_stats": {
            "stats": {
                "field": "properties.size"
            }
        },
        "size_percentiles": {
            "percentiles": {
                "field": "properties.size",
                "percents": [0.1, 1, 5, 25, 50, 75, 95, 99, 99.9],
                "keyed": "false"
            }
        },
        "datetime_percentiles": {
            "percentiles": {
                "field": "properties.datetime",
                "percents": [0.1, 1, 5, 25, 50, 75, 95, 99, 99.9],
                "keyed": "false"
            }
        },
        "datetime_stats": {
            "stats": {
                "field": "properties.datetime"
            }
        },
        "top_10_services_with_details" : {
            "terms" : { "field" : "properties.service", "size": 10 },
            "aggs": {
                "versions": {
                    "terms": { "field": "properties.version.raw", "size": 10 }
                },
                "formats": {
                    "terms": { "field": "properties.format", "size": 10 }
                },
                "crs": {
                    "terms": { "field": "properties.crs", "size": 10 }
                },
                "ows_resource": {
                    "terms": { "field": "properties.ows_resource.raw", "size": 100 }
                }
            }
        },
        "top_10_crs": {
            "terms": { "field": "properties.crs", "size": 10 }
        },
        "top_50_unique_ips_with_details" : {
            "terms" : { "field" : "properties.remote_host_ip", "size": 50 },
            "aggs": {
                "size_stats": {
                    "stats": {
                        "field": "properties.size"
                    }
                },
                "country": {
                    "terms": { "field": "properties.country.raw", "missing": "N/A" }
                },
                "top_10_ows_resources": {
                    "terms" : { "field" : "properties.ows_resource.raw", "size": 10 }
                }
            }
        },
        "top_10_formats": {
            "terms": { "field": "properties.format", "size": 10 }
        },
        "top_50_user_agents": {
            "terms": { "field": "properties.user_agent.raw", "size": 50 }
        },
        "top_50_countries" : {
            "terms" : { "field" : "properties.country.raw", "size": 50, "missing": "N/A" },
        },
        "top_100_cities" : {
            "terms" : { "field" : "properties.city.raw", "size": 100, "missing": "N/A" }
        },
        "top_100_ows_resources": {
            "terms" : { "field" : "properties.ows_resource.raw", "size": 100 }
        }
    }
}

def indexRecord(line, IP2LOCATION=[], server_source=None):
    """Converts an apache line to an ES index record with location information"""

    if isinstance(line, str):
        try:
            lr = OWSLogRecord(line)
        except (ValueError, NotFoundError):
            msg = 'Could not instantiate OWSLogRecord'
            LOGGER.warning(msg)
            raise NotFoundError(msg)
    else:
        lr = line

    location = locateIP(lr.ip_number, IP2LOCATION)

    index_record = {
        'type': 'Feature',
        'properties': {
            # LogRecord
            'remote_host_ip': lr.remote_host_ip,
            'ip_number': lr.ip_number,
            'city': location['city'],
            'province': location['province'],
            'country': location['country'],
            'country_code': location['country_code'],
            'datetime': lr.datetime,
            'timezone': lr.timezone,
            'request_type': lr.request_type,
            'request': lr.request,
            'protocol': lr.protocol,
            'status_code': lr.status_code,
            'size': lr.size,
            'referer': lr.referer,
            'user_agent': lr.user_agent,
            # OWSLogRecord
            'baseurl': lr.baseurl,
            'service': lr.service,
            'version': lr.version,
            'ows_request': lr.ows_request,
            'crs': lr.crs,
            'format': lr.format,
            'styles': lr.styles,
            'kvp': lr.kvp,
            'server_source': server_source,
            'ows_resource': lr.ows_resource
        },
        'geometry': {
            'type': 'point',
            'coordinates': [location['lon'], location['lat']]
        }
    }

    return index_record


def locateIP(ip_number, IP2LOCATION=[]):
    """Retrieve the loction information of an IP number with a lookup table from IP2LOCATION using binary search"""

    location = {
        'lat': None,
        'lon': None,
        'city': None,
        'country': None,
        'province': None,
        'country_code': None
    }

    if len(IP2LOCATION) is 0: # empty location lookup
        return location

    if ip_number == 0: # shortcut for unknown IP
         return IP2LOCATION[0]

    LOGGER.debug('Finding locaton for this ip_number: {}'.format(ip_number))

    notFound = True
    i = math.ceil(len(IP2LOCATION)/2) # start halfway
    i_min = 0
    i_max = len(IP2LOCATION)-1

    # binary search
    while notFound:
        if ip_number >= IP2LOCATION[i]['ipnum_start'] and ip_number <= IP2LOCATION[i]['ipnum_end']:
            notFound = False
            return IP2LOCATION[i]
        elif ip_number >= IP2LOCATION[i]['ipnum_start'] and ip_number > IP2LOCATION[i]['ipnum_end']:
            i_min = i
            i = math.ceil((i+i_max)/2) # bigger
        elif ip_number < IP2LOCATION[i]['ipnum_start'] and ip_number <= IP2LOCATION[i]['ipnum_end']:
            i_max = i
            i = math.floor((i+i_min)/2) # smaller
        else:
            notFound = False
            LOGGER.warning('Location not found for ip_number: {}'.format(ip_number))

    # None found
    return location

def parse_ip2location(ip2location_csv):
    """Parse out IP2LOCATION csv into a lookup list"""

    IP2LOCATION = []

    with open(ip2location_csv, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        for row in csv_reader:
            row_lookup = {
                "ipnum_start": int(row[0]),
                "ipnum_end": int(row[1]),
                "country_code": None if row[2] == '-' else row[2],
                "country": None if row[3] == '-' else row[3],
                "province": None if row[4] == '-' else row[4],
                "city": None if row[5] == '-' else row[5],
                "lat": float(row[6]),
                "lon": float(row[7])
            }
            IP2LOCATION.append(row_lookup)
    return IP2LOCATION

def download_ip2location():
    """Downloads the IP2LOCATION zip file to memory and returns the byte content"""

    ip2location_url = 'http://www.ip2location.com/download/?token={DOWNLOAD_TOKEN}&file={DATABASE_CODE}'
    r = requests.get(ip2location_url.format(DOWNLOAD_TOKEN=IP2LOCATION_TOKEN, DATABASE_CODE='DB5LITE'))
    return r.content

def unzip_ip2location(bytes_content):
    """Unzips a zipfile and extract out the IP2LOCATION.csv file to disk"""

    z = ZipFile(BytesIO(bytes_content))
    list_of_files = z.namelist()
    for filename in list_of_files:
        if filename.endswith('.CSV'):
            z.extract(filename, IP2LOCATION_EXTRACT_PATH) # write to disk
            return filename
    return None

def getErrors(res):
    """Gets ES errors from a res"""

    errRes = []
    for item in res['items']:
        if 'error' in item['index']:
            errRes.append(item)

    return errRes

def log_to_index(records=[], es_mapping={}, index_name='log_geomet_apache_records', IP2LOCATION=[], delete_existing=False, bulk_size=10000, refresh_index=False, create_id=False, server_source=None):
    """Puts a set of records into an ES Index and returns its results"""

    bulk_data = []
    linesParsed = 0
    totalParsed = 0
    numRecords = len(records)

    es = Elasticsearch()

    if delete_existing:
        # delete existing index if exists
        delete_index(index_name)

    # create index if it doesn't exist
    create_index(index_name, es_mapping)

    # bulk data insertion in batches
    for lr in records:
        try:
            data_dict = indexRecord(lr, IP2LOCATION, server_source)
        except (NotFoundError, ValueError):
            continue

        op_dict = {
            'index': {
                '_index': index_name,
                '_type': 'FeatureCollection'
            }
        }
        if create_id:
            op_dict['index']['_id'] = createHashId(lr)
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)
        linesParsed += 1

        # batch indexing
        if linesParsed >= bulk_size:
            totalParsed += linesParsed
            LOGGER.info('Bulk indexing: {} of {} total records'.format(totalParsed, numRecords))
            # bulk index the data in batches
            res = es.bulk(index=index_name, body=bulk_data, refresh=False, request_timeout=600)
            bulk_data = [] # reset for next iteration
            linesParsed = 0
            if res['errors']:
                errRes = getErrors(res)
                LOGGER.warning(errRes)

    # insert the remaining lines parsed if any
    if len(bulk_data) > 0:
        totalParsed += linesParsed
        LOGGER.info('Bulk indexing residual batch of {} out of {} total records'.format(totalParsed, numRecords))
        res = es.bulk(index=index_name, body=bulk_data, refresh=False, request_timeout=600)
        if res['errors']:
            errRes = getErrors(res)
            LOGGER.warning(errRes)

    if refresh_index:
        LOGGER.info('Refreshing index...')
        res = es.indices.refresh(index=index_name)

    return res

def create_index(index_name='geomet_apache_log', es_mapping={}):
    ''' Create index if it doesn't exist'''

    es = Elasticsearch()
    res = {}
    if not es.indices.exists(index_name):
        LOGGER.info('Creating new index: {}'.format(index_name))
        res = es.indices.create(index=index_name, body=es_mapping)

    return res

def delete_index(index_name='geomet_apache_log'):
    '''Delete an existing index if exists'''

    es = Elasticsearch()
    res = {}
    if es.indices.exists(index_name):
        LOGGER.info('Deleting existing index: {}'.format(index_name))
        res = es.indices.delete(index=index_name)
    
    return res

def createHashId(lr):
    '''Generates an ID hash for an ES document (row) from a given log record'''

    xstr = lambda s: '' if s is None else s
    # Can still exist multiple copies with the same IP, datetime and request combo
    id_str = xstr(lr.remote_host_ip) + xstr(lr.datetime.isoformat()) + xstr(lr.request)
    id_hash = hashlib.sha256(id_str.encode('utf-8')).hexdigest()
    return id_hash

def delete_es_records(index_name=None, date_from=None, date_to=None):
    '''Delete log records from an ES index between a date range. date_to and date_from must be of datetime type'''

    if index_name == None or date_to == None:
        return False

    es = Elasticsearch()

    query_delete = {
        "query": {
            "range": {
                "properties.datetime": {
                    "lt": date_to.strftime('%Y-%m-%dT%H:%M:%S')
                }
            }
        }
    }

    if date_from != None:
        query_delete['query']['range']['properties.datetime']['gte'] = date_from.strftime('%Y-%m-%dT%H:%M:%S')
    return es.delete_by_query(index=index_name, body=query_delete, request_timeout=6000)

def get_server_source(filepath):
    '''
    Returns the server host name portion of the log filename.
    
    format expected = /path1/path2/path3/server-num.{dot0}.{dot1}.{dot2}.{dot3}-YYYYMMDD*.gz
    '''

    return filepath.split('/')[4].split('_')[0]

def get_logfile_datetime_str(logfile_name):
    '''
    Returns the datetime string of the log file name
    
    logfile_name: format expected = /path1/path2/path3/server-num.{dot0}.{dot1}.{dot2}.{dot3}-YYYYMMDD*.gz

    returns the {YYYYMMDD*} of the filename
    '''

    return logfile_name.split('/')[4].split('.')[4].split('-')[1]

def get_yyyymmdd_str(log_yyyymmdd):
    '''
    Helper function following get_logfile_datetime_str()

    Returns yyyymmdd
    '''

    return log_yyyymmdd[:8] # yyyymmdd

def get_logfile_date(logfile_name):
    '''Returns the datetime of the logfile name at the YYYYMMDD level'''

    filename_datetime = get_logfile_datetime_str(logfile_name)
    filename_date = get_yyyymmdd_str(filename_datetime)
    file_datetime = datetime.strptime(filename_date, '%Y%m%d')

    return file_datetime

def get_latest_logs(logfiles):
    '''Grabs only the logs of the latest available day from a list of log files'''

    latest_logfiles = []
    logfiles.sort(key = lambda filename: get_logfile_datetime_str(filename), reverse=True)
    latest_date_ = get_logfile_datetime_str(logfiles[0])
    latest_date = get_yyyymmdd_str(latest_date_) #YYYYMMDD

    for filename in logfiles:
        file_date = get_yyyymmdd_str(get_logfile_datetime_str(filename))
        if file_date == latest_date:
            latest_logfiles.append(filename)

    return latest_logfiles


@click.group()
def log_es():
    pass

@click.command()
@click.argument('filepattern', # glob pattern of log files to parse
            type=click.Path())
@click.option('--ip2location-file',
            default=IP2LOCATION_PATH,
            show_default=True,
            type=click.Path(exists=False, resolve_path=True),
            help='Filepath to IP2LOCATION csv')
@click.option('--index-name',
            default=ES_INDEX_NAME_LOGS_ALL,
            show_default=True,
            help='ES index name for individual log records')
@click.option('--index-daily-name',
            default=ES_INDEX_NAME_LOGS_DAILY,
            show_default=True,
            help='ES index name for daily stats')
@click.option('--delete-existing-logs',
            default=False,
            show_default=True,
            is_flag=True,
            help='Deletes the existing index before parsing log files into ES index')
@click.option('--delete-existing-daily',
            default=False,
            show_default=True,
            is_flag=True,
            help='Deletes the existing daily stat index before parsing log files into ES index')
@click.option('--bulk-size',
            default=10000,
            show_default=True,
            type=int,
            help='ES bulk size for indexing')
@click.option('--verbosity', type=click.Choice(['ERROR', 'WARNING',
              'INFO', 'DEBUG']), help='Verbosity')
@click.option('--skip-daily', 'skip_daily_stats',
            default=False,
            show_default=True,
            is_flag=True,
            help='Option to skip calculating and indexing daily statistics of parsed logs')
@click.option('--keep-log-days',
            default=None,
            show_default=True,
            type=int,
            help='Specify number of days past to keep indivdual log records as the parsing continues')
@click.option('--cutoff',
            default=None,
            show_default=True,
            type=int,
            help='Cutoff point for number of records to stop parsing for each log file. Meant for testing.')
@click.option('--create-id',
            default=False,
            show_default=True,
            is_flag=True,
            help='Flag to set each log record to have an ID hash to avoid duplicate log entries')
@click.option('--latest-day',
            default=False,
            show_default=True,
            is_flag=True,
            help='Flag to parse only the logs of the last available day matching FILEPATTERN')
def index(filepattern, ip2location_file, index_name, index_daily_name, delete_existing_logs, delete_existing_daily, verbosity, bulk_size, skip_daily_stats, keep_log_days, cutoff, create_id, latest_day):
    '''
    Parses http access logs to an ES index. 

    Must pass in filepath pattern to log files as an argument. Remember to escape special character that would register in Bash with "\\". 

    Example command:$ GeoUsage log-es index /path/to/logs-server-0\\*/access_server_num_0\\*_log-2019\\[01\\|02\\]\\*.gz
    '''

    click.clear()

    logfiles = glob.glob(filepattern)

    if verbosity is not None:
        logging.basicConfig(level=getattr(logging, verbosity))
        click.secho('\nVerbosity set to: ', nl=False)
        click.secho('{}'.format(verbosity), fg='cyan')
    else:
        logging.getLogger(__name__).addHandler(logging.NullHandler())

    click.secho('\nIndexing log files matching this pattern: ', nl=False)
    click.secho('{}'.format(filepattern), fg='cyan')

    if len(logfiles) > 0:
        if skip_daily_stats:
            click.secho('\nSkipping creation of daily statistics.', fg='cyan')
        else:
            click.secho('\nCreate daily statistics too.')

        # sort logfiles by datetime string of the file name only
        logfiles.sort(key = lambda filename: get_logfile_datetime_str(filename))

        logtrack = {
            'last_filename_datetime': None,
            'base_filename_datetime': None,
            'start_date_daily': None,
            'end_date_daily': None,
            'threshold_date': None
        }

        if cutoff is not None:
            click.secho('\nCutoff set after parsing', nl=False)
            click.secho(' {}'.format(cutoff), nl=False, fg='cyan')
            click.secho(' records per log file.')

        click.secho('\nApache log files found from filepattern:', fg='green', bold=True)
        for filename in logfiles:
            click.secho('{}'.format(filename), fg='cyan')

        if latest_day:
            logfiles = get_latest_logs(logfiles)
            click.secho('\nHere are the latest logs of that list:')
            for filename in logfiles:
                click.secho('{}'.format(filename), fg='cyan')

        if delete_existing_logs:
            # delete existing index if exists
            if click.confirm(click.style('\nAre you sure you want to delete ES index named: ') +
                click.style('{}'.format(index_name), fg='cyan'), abort=True):
                # delete existing index if exists
                res = delete_index(index_name)
                if 'acknowledged' in res:
                    if res['acknowledged']:
                        click.secho('\nSuccessfully deleted ES index: ', fg='green', nl=False)
                        click.secho('{}'.format(index_name), fg='cyan')
                    else:
                        click.secho('\nFailed to delete ES index: ', fg='red', nl=False)
                        click.secho('{}'.format(index_name), fg='cyan')
                else:
                    click.secho('\nIndex name not found for deletion: ', nl=False)
                    click.secho('{}'.format(index_name), fg='cyan')

        if delete_existing_daily:
            # delete existing daily stat index if exists
            if click.confirm(click.style('\nAre you sure you want to delete ES index named: ') +
                click.style('{}'.format(index_daily_name), fg='cyan'), abort=True):
                # delete existing index if exists
                res = delete_index(index_daily_name)
                if 'acknowledged' in res:
                    if res['acknowledged']:
                        click.secho('\nSuccessfully deleted ES index: ', fg='green', nl=False)
                        click.secho('{}'.format(index_daily_name), fg='cyan')
                    else:
                        click.secho('\nFailed to delete ES index: ', fg='red', nl=False)
                        click.secho('{}'.format(index_daily_name), fg='cyan')
                else:
                    click.secho('\nIndex name not found for deletion: ', nl=False)
                    click.secho('{}'.format(index_daily_name), fg='cyan')

        if os.path.isfile(ip2location_file) == False:
            click.secho('\nIP2LOCATION lookup file was not found. Downloading and unzipping IP2LOCATION zipfile to:', nl=False)
            click.secho(' {}'.format(IP2LOCATION_EXTRACT_PATH), fg='cyan')
            unzip_ip2location(download_ip2location())
            click.secho('Done', fg='green')

        click.secho('\nParsing IP2LOCATION lookup file: ', nl=False)
        click.secho('{}'.format(ip2location_file), fg='cyan')
        IP2LOCATION = parse_ip2location(ip2location_file)

        es = Elasticsearch()
        create_index(index_daily_name, LOG_DAILY_AGG_SETTINGS_MAPPINGS)
    
        with click.progressbar(logfiles, 
                label='Log files indexed', 
                length=len(logfiles)) as bar:
            for filename in bar:
                if filename.endswith('gz'):
                    open_ = gzip.open
                else:
                    open_ = open

                records = []
                click.secho('\nParsing log file:', nl=False)
                click.secho(' {}'.format(filename), fg='cyan', nl=False)
                click.secho(' and bulk indexing', nl=False)
                click.secho(' {}'.format(bulk_size), fg='cyan', nl=False)
                click.secho(' records at a time to ES index:', nl=False)
                click.secho(' {}'.format(index_name), fg='cyan')

                with open_(filename, 'rt') as ff:
                    file_datetime = get_logfile_date(filename)
                    server_source = get_server_source(filename)

                    if logtrack['base_filename_datetime'] is None: # initialize
                        logtrack['base_filename_datetime'] = file_datetime
                        logtrack['start_date_daily'] = file_datetime - timedelta(days=1)
                        logtrack['end_date_daily'] = file_datetime
                        logtrack['threshold_date'] = file_datetime + timedelta(days=1) # buffer 1 days of records

                    base_yyyymmdd = logtrack['start_date_daily'].strftime('%Y-%m-%d')
                    click.secho('\nParsing log file date: ', nl=False)
                    click.secho('{} '.format(file_datetime.strftime('%Y-%m-%d')), fg='cyan', nl=False)
                    click.secho('| Date threshold to start analyze: ', nl=False)
                    click.secho('{}'.format(logtrack['threshold_date'].strftime('%Y-%m-%d')), fg='cyan')

                    # Parse logfile
                    linesparsed = 0
                    for line in ff:
                        try:
                            lr = OWSLogRecord(line)
                        except (ValueError, NotFoundError):
                            continue # skip parsing this line record

                        records.append(lr)
                        linesparsed += 1

                        if len(records) >= bulk_size:
                            click.secho('.', nl=False, fg='green')
                            log_to_index(records, LOG_SETTINGS_MAPPINGS, index_name, IP2LOCATION, False, bulk_size, False, create_id, server_source)
                            records = [] # reset for next iteration

                        if cutoff is not None:
                            if linesparsed >= cutoff:
                                click.secho(' [Forced cutoff after parsing', nl=False)
                                click.secho(' {}'.format(cutoff), nl=False, fg='cyan')
                                click.secho(' records] ', nl=False)
                                break
                    if len(records) >= 1: # last remaining records
                        click.secho('.', nl=False, fg='green')
                        log_to_index(records, LOG_SETTINGS_MAPPINGS, index_name, IP2LOCATION, False, bulk_size, False, create_id, server_source)

                    logtrack['last_filename_datetime'] = file_datetime
                    click.secho('done\n', nl=False, fg='green', bold=True)

                    # When parsed log file is passed 1 day of parsed records or is the last file, start analyzing
                    if skip_daily_stats:
                        pass
                    elif (file_datetime >= logtrack['threshold_date'] or filename == logfiles[-1]):
                        start_datestr = logtrack['start_date_daily'].strftime('%Y-%m-%dT%H:%M:%S')
                        end_datestr = logtrack['end_date_daily'].strftime('%Y-%m-%dT%H:%M:%S')

                        click.secho('\nDate threshold reached! Start ES querying for daily stats...', nl=False)

                        query_daily = ES_QUERY_DAILY_LOGS.copy()
                        query_daily['query']['range']['properties.datetime']['gte'] = start_datestr
                        query_daily['query']['range']['properties.datetime']['lt'] = end_datestr

                        es.indices.refresh(index=index_name)
                        res_daily = es.search(index=index_name, body=query_daily, request_timeout=6000)

                        if res_daily['_shards']['successful'] == 1:
                            click.secho('done', fg='green')
                            click.secho('\nSuccessfully queried ', nl=False)
                            click.secho('{} '.format(res_daily['hits']['total']), fg='cyan', nl=False)
                            click.secho('records to make daily stats for ', fg='green', nl=False)
                            click.secho('{}. '.format(base_yyyymmdd), fg='cyan')
                            
                            # Index resulting daily stats
                            index_daily_record = {
                                'date': base_yyyymmdd,
                                'date_min': res_daily['aggregations']['datetime_stats']['min_as_string'],
                                'date_max': res_daily['aggregations']['datetime_stats']['max_as_string'],
                                'total_requests': res_daily['hits']['total'],
                                'size_stats': res_daily['aggregations']['size_stats'],
                                'datetime_stats': res_daily['aggregations']['datetime_stats'],
                                'unique_ips': res_daily['aggregations']['unique_ips_count']['value'],
                                'size_percentiles': res_daily['aggregations']['size_percentiles']['values'],
                                'datetime_percentiles': res_daily['aggregations']['datetime_percentiles']['values'],
                                'top_50_user_agents': res_daily['aggregations']['top_50_user_agents'],
                                'top_50_unique_ips_with_details': res_daily['aggregations']['top_50_unique_ips_with_details'],
                                'top_10_services_with_details': res_daily['aggregations']['top_10_services_with_details'],
                                'top_10_crs': res_daily['aggregations']['top_10_crs'],
                                'top_100_cities': res_daily['aggregations']['top_100_cities'],
                                'top_50_countries': res_daily['aggregations']['top_50_countries'],
                                'top_10_ows_request_types': res_daily['aggregations']['top_10_ows_request_types']
                            }

                            click.secho('\nIndexing stats...', nl=False)
                            res = es.index(index=index_daily_name, doc_type='stats', body=index_daily_record, id=base_yyyymmdd, refresh='true', request_timeout=600)

                            if res['_shards']['successful'] == 1:
                                click.secho('done', fg='green')
                            else:
                                click.secho('unsuccessful', fg='red')

                            if keep_log_days != None:
                                datetime_x_days_ago = logtrack['base_filename_datetime'] - timedelta(days=keep_log_days)
                                click.secho('\nDeleting log records before ', nl=False)
                                click.secho('{}'.format(datetime_x_days_ago.strftime('%Y-%m-%d')), fg='cyan', nl=False)
                                click.secho('...', nl=False)
                                res = delete_es_records(index_name, None, datetime_x_days_ago)
                                if res['deleted'] > 0:
                                    click.secho('Done. Deleted ', fg='green', nl=False)
                                    click.secho('{} '.format(res['deleted']), fg='cyan', nl=False)
                                    click.secho('log records from ', fg='green', nl=False)
                                    click.secho('{}'.format(index_name), fg='cyan')
                                    es.indices.refresh(index=index_name)
                                else:
                                    click.secho('\nNo log records to delete before ', nl=False)
                                    click.secho('{}'.format(datetime_x_days_ago.strftime('%Y-%m-%d')), fg='cyan')
                        else:
                            click.secho('\nFailed to query daily stats for ', fg='red', nl=False)
                            click.secho('{}'.format(base_yyyymmdd), fg='cyan')

                        click.echo('\n')
                        logtrack['base_filename_datetime'] = logtrack['end_date_daily']
                        logtrack['start_date_daily'] = logtrack['end_date_daily']
                        logtrack['end_date_daily'] = logtrack['end_date_daily'] + timedelta(days=1)
                        logtrack['threshold_date'] = file_datetime + timedelta(days=1) # buffer 1 days of records

        click.secho('\nRefreshing index...', fg='cyan')
        es.indices.refresh(index=index_name)
        click.secho('\nSuccessfully indexed all (', fg='green', nl=False)
        click.secho('{}'.format(len(logfiles)), fg='cyan', nl=False)
        click.secho(') log files!\n'.format(len(logfiles)), fg='green')
    else:
        click.secho('\nCould not find log files matching that file pattern.\n', fg='red')


@click.command()
@click.argument('index_name', nargs=1)
def delete(index_name):
    '''Delete a particular ES index with a given name as argument'''

    es = Elasticsearch()

    if click.confirm('Are you sure you want to delete ES index named: {}'.format(click.style(index_name, fg='cyan')), abort=True):
        # delete existing index if exists
        res = delete_index(index_name)
        if 'acknowledged' in res:
            if res['acknowledged']:
                click.secho('\nSuccessfully deleted ES index: {}'.format(index_name), fg='green', bold=True)
            else:
                click.secho('\nFailed to delete ES index: {}'.format(index_name), fg='red', bold=True)
        else:
            click.secho('\nIndex name not found for deletion: {}'.format(index_name), bold=True)

@click.command()
@click.option('--date-from', 'date_from',
            default=None,
            required=False,
            help='Start date in the form {YYYY}-{MM}-{DD}T{HH}:{MM}:{SS}.')
@click.option('--date-to', 'date_to',
            required=False,
            help='End date in the form {YYYY}-{MM}-{DD}T{HH}:{MM}:{SS}.')
@click.option('--past-days', 'past_days',
            required=False,
            show_default=True,
            default=21,
            type=int,
            help='Clear all logs leading up to the {# of days from today}. This option cannot be combined with --date-from and --date-to.')
@click.option('--force', '-f', 'force',
            default=False,
            show_default=True,
            is_flag=True,
            help='Force to clear logs without any prompts')
def clear_logs(date_from, date_to, past_days, force):
    '''Clears ES log records between a date range'''

    index_name = ES_INDEX_NAME_LOGS_ALL

    click.clear()

    if date_to != None or date_from != None:
        try:
            date_to = datetime.strptime(date_to, '%Y-%m-%dT%H:%M:%S')
            date_from = datetime.strptime(date_from, '%Y-%m-%dT%H:%M:%S') if date_from != None else None
        except (TypeError, ValueError):
            click.secho('Your specified dates are invalid. Please ensure your date is in the format: {YYYY}-{MM}-{DD}T{HH}:{MM}:{SS}', fg='red')
            raise click.Abort
    else:
        date_to = datetime.today() - timedelta(days=past_days)

    def execute_clear_logs():
        '''Perform the ES query to delete logs between given date range'''

        res = delete_es_records(index_name, date_from, date_to)
        if res['deleted'] > 0:
            click.secho('\nDone. Deleted ', fg='green', nl=False)
            click.secho('{} '.format(res['deleted']), fg='cyan', nl=False)
            click.secho('log records from ', fg='green', nl=False)
            click.secho('{}'.format(index_name), fg='cyan')
            es.indices.refresh(index=index_name)
        else:
            click.secho('\nNo log records to delete for your specified dates.')

    es = Elasticsearch()
        
    if force:
        execute_clear_logs()
    else:
        msg_prompt = click.style('Are you sure you want to delete log records from ES index named:')
        msg_prompt += click.style(' {}'.format(index_name), fg='cyan')

        if date_from != None:
            msg_prompt += click.style('\nfor dates greater or equal to (>=)')
            msg_prompt += click.style(' {}'.format(date_from), fg='cyan')
            msg_prompt += click.style(' and less than (<)')
            msg_prompt += click.style(' {}'.format(date_to), fg='cyan')
        else:
            msg_prompt += click.style('\nfor all dates less than (<)')
            msg_prompt += click.style(' {}'.format(date_to), fg='cyan')
        msg_prompt += click.style('?')
        
        if click.confirm(msg_prompt, abort=True):
            execute_clear_logs()
        
log_es.add_command(index)
log_es.add_command(delete)
log_es.add_command(clear_logs)
