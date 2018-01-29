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

__version__ = '0.1.0'

from datetime import datetime
import logging

import click

LOGGER = logging.getLogger(__name__)


class LogRecord(object):
    """Generic Log Record"""
    def __init__(self, line):
        """
        Initialize a LogRecord object

        :param line: access log record line

        :returns: GeoUsage.LogRecord instance
        """

        self._line = line.strip()
        """raw line / record"""

        self.remote_host = None
        """remote host/IP"""

        self.datetime = None
        """datetime.datetime object of request"""

        self.timezone = None
        """timezone request"""

        self.request_type = None
        """type of HTTP request (GET, POST, etc.)"""

        self.request = None
        """HTTP request"""

        self.protocol = None
        """protocol and version of request"""

        self.status_code = 0
        """HTTP response status code"""

        self.size = 0
        """size of HTTP response"""

        self.referer = None
        """Referer"""

        self.user_agent = None
        """User-Agent"""

        LOGGER.debug('Parsing log record')
        tokens = self._line.split()

        self.remote_host = tokens[0]

        self.datetime = datetime.strptime(tokens[3].lstrip('['),
                                          '%d/%b/%Y:%H:%M:%S')
        self.timezone = tokens[4].rstrip(']')
        self.request_type = tokens[5].lstrip('"')
        self.request = tokens[6]
        self.protocol = tokens[7].rstrip('"')
        self.status_code = int(tokens[8])
        self.size = int(tokens[9])
        self.referer = tokens[10].replace('"', '')
        self.user_agent = tokens[11].lstrip('"')

    def __repr__(self):
        return '<LogRecord> {}'.format(self.request)


class OWSLogRecord(LogRecord):
    """OWS Log Record"""
    def __init__(self, line, service_type=None):
        """
        Initialize an OWSLogRecord object

        :param line: access log record line

        :returns: GeoUsage.OWSLogRecord instance
        """

        self.baseurl = None
        """base URL"""

        self.service = None
        """OWS service type (OGC:WMS, OGC:WFS, OGC:WCS)"""

        self.version = None
        """OWS version"""

        self.ows_request = None
        """OWS request"""

        self.kvp = {}
        """keyword/value of request parameters and values"""

        LogRecord.__init__(self, line)

        LOGGER.debug('Splitting OWS request line')
        self.baseurl, _kvps = self.request.split('?')
        for _kvp in _kvps.split('&'):
            LOGGER.debug('keyword/value pair: {}'.format(_kvp))
            if '=' in _kvp:
                k, v = _kvp.split('=')
                self.kvp[k.lower()] = v

        if 'service' in self.kvp:
            self.service = self.kvp['service']
        if 'version' in self.kvp:
            self.version = self.kvp['version']
        if 'request' in self.kvp:
            self.ows_request = self.kvp['request']

        if service_type is not None:
            if self.service is not None and service_type != self.service:
                msg = 'Service type {} not found'.format(service_type)
                LOGGER.exception(msg)
                raise NotFound(msg)


class WMSLogRecord(OWSLogRecord):
    """OGC:WMS Log Record"""
    def __init__(self, line):
        """
        Initialize an OWSLogRecord object

        :param line: access log record line

        :returns: GeoUsage.OWSLogRecord instance
        """

        self.resource = 'layers'
        """WMS parameter to identify resource"""

        try:
            OWSLogRecord.__init__(self, line, service_type='OGC:WMS')
        except NotFound as err:
            LOGGER.error(err)
            return None


class Analyzer(object):
    """Log Analyzer"""
    def __init__(self, records=[]):
        """
        Initialize an Analyzer object
        :param records: list of LogRecord objects

        :returns: GeoUsage.Analyzer instance
        """

        self.start = None
        """start `datetime.datetime` of analysis"""

        self.end = None
        """end `datetime.datetime` of analysis"""

        self.requests = {}
        """OWS requests and counts"""

        self.total_requests = 0
        """total OWS request counts"""

        self.total_size = 0
        """total transfer (bytes)"""

        self.resources = {}
        """resources requested and counts"""

        self.user_agents = {}
        """user agents and counts"""

        LOGGER.info('Analyzing {} records'.format(len(records)))

        self.start = min(item.datetime for item in records)
        self.end = max(item.datetime for item in records)

        for r in records:
            LOGGER.debug('Analyzing OWS requests')
            if r.ows_request is not None:
                if r.ows_request in self.requests:
                    self.requests[r.ows_request] += 1
                else:
                    self.requests[r.ows_request] = 1

            LOGGER.debug('Analyzing total bytes transferred')
            self.total_size += r.size

            LOGGER.debug('Analyzing User Agents')
            if r.user_agent in self.user_agents:
                self.user_agents[r.user_agent] += 1
            else:
                self.user_agents[r.user_agent] = 1

            if r.resource in r.kvp:
                resource_name = r.kvp[r.resource]
                if resource_name in self.resources:
                    self.resources[resource_name] += 1
                else:
                    self.resources[resource_name] = 1

        self.total_requests = sum(item for item in self.requests.values())
        self.requests = sorted(self.requests.items(),
                               key=lambda x: x[1], reverse=True)
        self.resources = sorted(self.resources.items(),
                                key=lambda x: x[1], reverse=True)


class NotFound(Exception):
    """Value not found Exception"""
    pass


@click.group()
def log():
    pass


@click.command()
@click.pass_context
@click.option('--logfile', '-l',
              type=click.Path(exists=True, resolve_path=True),
              help='logfile to parse')
@click.option('--verbosity', type=click.Choice(['ERROR', 'WARNING',
              'INFO', 'DEBUG']), help='Verbosity')
def analyze(ctx, logfile, verbosity, service_type='OGC:WMS'):
    """parse http access log"""

    records = []

    if verbosity is not None:
        logging.basicConfig(level=getattr(logging, verbosity))
    else:
        logging.getLogger(__name__).addHandler(logging.NullHandler())

    if logfile is None:
        raise click.UsageError('Missing --logfile argument')

    with open(logfile) as ff:
        for line in ff.readlines():
            r = WMSLogRecord(line)
            records.append(r)

    a = Analyzer(records)

    click.echo('\nGeoUsage Analysis\n')
    click.echo('Logfile: {}'.format(logfile))
    click.echo('Period: {} - {}\n'.format(a.start.isoformat(),
                                          a.end.isoformat()))
    click.echo('Total bytes transferred: {}\n'.format(a.total_size))
    click.echo('Requests breakdown ({}):'.format(a.total_requests))
    for req in a.requests:
        click.echo('    {}: {}'.format(req[0], req[1]))
    click.echo('\nMost Requested data breakdown:')
    for res in a.resources:
        click.echo('    {}: {}'.format(res[0], res[1]))


log.add_command(analyze)
