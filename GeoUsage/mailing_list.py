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

import logging
import os
import re

import click
import requests

LOGGER = logging.getLogger(__name__)


class MailmanAdmin:
    """Mailman admin interface"""
    def __init__(self, admin_url, admin_password):
        """
        Initialize a MailmanAdmin object

        :param admin_url: Mailman admin URL"
        :param admin_password: Mailman admin password"

        :returns: GeoUsage.MailmanAdmin object
        """

        self.url = admin_url
        """Mailman admin URL"""

        self.password = admin_password
        """Mailman admin password"""

    @property
    def member_count(self):
        """
        get total number of members

        :returns: total number of members
        """

        url = '{}/members'.format(self.url)
        headers = {
            'User-Agent': 'GeoUsage (https://github.com/geopython/GeoUsage)'
        }

        LOGGER.debug('Fetching URL: {}'.format(url))
        response = requests.post(url,
                                 headers=headers,
                                 data={'adminpw': self.password})
        LOGGER.debug('Parsing HTML')

        element = re.search(r'(\d+) members total', response.text).group(0)
        members = int(element.split('members total')[0].strip())

        return members


@click.group()
def mailing_list():
    pass


@click.command()
@click.pass_context
@click.option('--verbosity', type=click.Choice(['ERROR', 'WARNING',
              'INFO', 'DEBUG']), help='Verbosity')
def member_count(ctx, verbosity):
    """get total number of members"""

    if verbosity is not None:
        logging.basicConfig(level=getattr(logging, verbosity))
    else:
        logging.getLogger(__name__).addHandler(logging.NullHandler())

    ma = MailmanAdmin(os.environ['GEOUSAGE_MAILMAN_ADMIN_URL'],
                      os.environ['GEOUSAGE_MAILMAN_ADMIN_PASSWORD'])

    click.echo(ma.member_count)


mailing_list.add_command(member_count)
