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
import gzip
import logging
import socket

import click

LOGGER = logging.getLogger(__name__)

SERVICE_TYPES = {
    'OGC:WMS': 'WMS',
    'OGC:WFS': 'WFS'
}


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

        self.remote_host_ip = None
        """remote host (IP)"""

        self.remote_host_ip = None
        """remote host (hostname)"""

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

        self.remote_host_ip = tokens[0]

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
    def __init__(self, line, endpoint=None, service_type=None):
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

        LOGGER.debug('Analyzing URL match')

        if endpoint is not None and not self.request.startswith(endpoint):
            msg = 'endpoint not found'
            LOGGER.warning(msg)
            raise NotFoundError(msg)

        LOGGER.debug('Splitting OWS request line')

        try:
            self.baseurl, _kvps = self.request.split('?')
        except ValueError:
            msg = 'Non OWS URL'
            LOGGER.warning(msg)
            raise NotFoundError(msg)

        for _kvp in _kvps.split('&'):
            LOGGER.debug('keyword/value pair: {}'.format(_kvp))
            if '=' in _kvp:
                k, v = _kvp.split('=', 1)
                self.kvp[k.lower()] = v

        if 'service' in self.kvp:
            self.service = self.kvp['service']
        if 'version' in self.kvp:
            self.version = self.kvp['version']
        if 'request' in self.kvp:
            self.ows_request = self.kvp['request']

        if service_type is not None:
            if service_type in SERVICE_TYPES:
                service_type_ = SERVICE_TYPES[service_type]
                if self.service is not None and service_type_ != self.service:
                    msg = 'Service type {} not found'.format(service_type_)
                    LOGGER.error(msg)
                    raise NotFoundError(msg)

    def __repr__(self):
        return '<OWSLogRecord> {}'.format(self.request)


class WMSLogRecord(OWSLogRecord):
    """OGC:WMS Log Record"""
    def __init__(self, line, endpoint=None):
        """
        Initialize an OWSLogRecord object

        :param line: access log record line

        :returns: GeoUsage.OWSLogRecord instance
        """

        self.resource = 'layers'
        """WMS parameter to identify resource"""

        OWSLogRecord.__init__(self, line, endpoint=endpoint,
                              service_type='OGC:WMS')

    def __repr__(self):
        return '<OWSLogRecord> {}'.format(self.request)


class Analyzer(object):
    """Log Analyzer"""
    def __init__(self, records=[], resolve_ips=False):
        """
        Initialize an Analyzer object
        :param records: list of LogRecord objects
        :param resolve_ips: resolve IPs (boolean)

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

        self.unique_ips = {}
        """unique visitors"""

        self.resources = {}
        """resources requested and counts"""

        self.user_agents = {}
        """user agents and counts"""

        LOGGER.info('Analyzing {} records'.format(len(records)))

        if len(records) == 0:
            LOGGER.info('No records to analyze')
            return

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

            LOGGER.debug('Analyzing data usage')
            if r.resource in r.kvp:
                resource_name = r.kvp[r.resource]
                if resource_name in self.resources:
                    self.resources[resource_name] += 1
                else:
                    self.resources[resource_name] = 1

            LOGGER.debug('Analyzing unique IP addresses')
            if r.remote_host_ip in self.unique_ips:
                self.unique_ips[r.remote_host_ip]['count'] += 1
            else:
                self.unique_ips[r.remote_host_ip] = {'count': 1}
                if resolve_ips:
                    try:
                        a = socket.gethostbyaddr(r.remote_host_ip)
                        hostname = a[0]
                    except socket.herror:
                        hostname = None
                    self.unique_ips[r.remote_host_ip]['hostname'] = hostname
                else:
                    self.unique_ips[r.remote_host_ip]['hostname'] = None

        LOGGER.debug('Analyzing total requests')
        self.total_requests = sum(item for item in self.requests.values())

        self.requests = sorted(self.requests.items(),
                               key=lambda x: x[1], reverse=True)
        self.resources = sorted(self.resources.items(),
                                key=lambda x: x[1], reverse=True)
        self.unique_ips = sorted(self.unique_ips.items(),
                                 key=lambda x: x[1]['count'], reverse=True)

    def __repr__(self):
        return '<Analyzer>'


def parse_iso8601(value):
    """
    Convenience function to parse ISO8601
    time instant or range

    :param: input time string

    :returns: list of either time instant or range
    """

    time_values = []

    time_tokens = value.split('/')
    LOGGER.debug('{} time tokens found'.format(len(time_tokens)))
    for tt in time_tokens:
        if 'T' in tt:  # YYYY-MM-DDTHH:MM:SS
            LOGGER.debug('datetime found')
            t = datetime.strptime(tt, '%Y-%m-%dT%H:%M:%S')
        else:
            LOGGER.debug('date found')
            t = datetime.strptime(tt, '%Y-%m-%d')
        time_values.append(t)

    return time_values


def test_time(intime, times, datetype='date'):
    """
    Tests intime against a time instant or time range

    :param intime: `datetime.datetime` object
    :param times: list of `datetime.datetime` objects
    :param datetype: type of `datetime` object (`date` or `datetime`)

    :returns: boolean of whether time matches or is in range
    """

    result = False

    if len(times) == 1:  # time instant comparison
        LOGGER.debug('comparing time instant')
        if datetype == 'datetime':  # datetime.datetime comparison
            LOGGER.debug('comparing datetime instant')
            result = intime == times[0]
        else:  # datetime.date comparison
            LOGGER.debug('comparing date instant')
            result = intime.date() == times[0].date()
    else:  # time range comparison
        LOGGER.debug('comparing time range')
        if datetype == 'datetime':  # datetime.datetime comparison
            LOGGER.debug('comparing datetime range')
            result = intime <= times[1] and intime >= times[0]
        else:  # datetime.date comparison
            LOGGER.debug('comparing date range')
            result = (intime.date() <= times[1].date() and
                      intime.date() >= times[0].date())

    return result


class NotFoundError(Exception):
    """Value not found Exception"""
    pass


@click.group()
def log():
    pass


@click.command()
@click.pass_context
@click.option('--endpoint', '-e', help='OWS endpoint (base URL)')
@click.option('--logfile', '-l',
              type=click.Path(exists=True, resolve_path=True),
              help='logfile to parse')
@click.option('--resolve-ips', '-r', 'resolve_ips', default=False,
              is_flag=True, help='resolve IP addresses')
@click.option('--service-type', '-s', 'service_type',
              type=click.Choice(['OGC:WMS', 'OGC:WCS']), help='service type')
@click.option('--time', '-t', 'time_',
              help='time filter (ISO8601 instance or start/end)')
@click.option('--verbosity', type=click.Choice(['ERROR', 'WARNING',
              'INFO', 'DEBUG']), help='Verbosity')
def analyze(ctx, logfile, endpoint, verbosity, resolve_ips=False,
            service_type='OGC:WMS', time_=None):
    """parse http access log"""

    records = []
    time__ = []

    if verbosity is not None:
        logging.basicConfig(level=getattr(logging, verbosity))
    else:
        logging.getLogger(__name__).addHandler(logging.NullHandler())

    if logfile is None:
        raise click.UsageError('Missing --logfile argument')

    if time_ is not None:
        time__ = parse_iso8601(time_)

    if logfile.endswith('gz'):
        open_ = gzip.open
    else:
        open_ = open
    with open_(logfile, 'rt') as ff:
        for line in ff.readlines():
            try:
                r = WMSLogRecord(line, endpoint=endpoint)
                if time_ is not None:
                    if test_time(r.datetime, time__):
                        LOGGER.debug('Adding line based on time filter')
                        records.append(r)
                    else:
                        LOGGER.debug('Skipping line based on time filter')
                else:
                    records.append(r)
            except NotFoundError:
                pass

    if len(records) == 0:
        raise click.ClickException('No records to analyze')

    a = Analyzer(records, resolve_ips=resolve_ips)

    click.echo('\nGeoUsage Analysis')
    click.echo('=================\n')
    click.echo('Logfile: {}\n'.format(logfile))
    click.echo('Period: {} - {}\n'.format(a.start.isoformat(),
                                          a.end.isoformat()))
    click.echo('Total bytes transferred: {}\n'.format(a.total_size))
    click.echo('Unique visitors ({}):'.format(len(a.unique_ips)))
    for req in a.unique_ips:
        click.echo('    {} ({}): {}'.format(req[0], req[1]['hostname'],
                                            req[1]['count']))
    click.echo('\nRequests ({}):'.format(a.total_requests))
    for req in a.requests:
        click.echo('    {}: {}'.format(req[0], req[1]))
    click.echo('\nRequested data:'.format(len(a.resources)))
    for res in a.resources:
        click.echo('    {}: {}'.format(res[0], res[1]))


log.add_command(analyze)
