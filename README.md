# GeoUsage
[![Build Status](https://github.com/geopython/GeoUsage/workflows/build%20%E2%9A%99%EF%B8%8F/badge.svg)](https://github.com/geopython/GeoUsage/actions)

Metrics Analysis for OGC Web Services

## Overview

GeoUsage is a pure Python package providing OGC Web Services usage analysis.

## Installation

The easiest way to install GeoUsage is via [pip](https://pip.pypa.io/en/stable/):

```bash
pip install GeoUsage
```

This assumes you have the privileges to install GeoUsage on your system
which may require administrator/root privileges.  For isolated installations,
see [Installing GeoUsage in a virtualenv](#installing-geousage-in-a-virtualenv).

### Requirements
- Python 3
- [virtualenv](https://virtualenv.pypa.io/)

### Dependencies
Dependencies are listed in [requirements.txt](requirements.txt). Dependencies
are automatically installed during GeoUsage installation.

### Installing GeoUsage in a virtualenv

Using a virtualenv allows for isolated installations which do not affect
system wide dependencies which require administrative/root privileges.  To
install to a virtualenv, perform the following steps:

```bash
# setup virtualenv
python3 -m venv GeoUsage
cd GeoUsage
source bin/activate

# clone codebase and install
git clone https://github.com/geopython/GeoUsage.git
cd GeoUsage
python3 setup.py install
```

## Running

```bash
cp GeoUsage-config.env local.env
vi local.env # update environment variables accordingly

# help
GeoUsage --help

# get version
GeoUsage --version

# perform WMS analysis on an Apache logfile on any WMS endpoint
GeoUsage log analyze </path/to/apache_logfile> --service-type=OGC:WMS

# perform WMS analysis on an Apache logfile on a specific WMS endpoint
GeoUsage log analyze </path/to/apache_logfile> --service-type=OGC:WMS --endpoint=/ows

# perform WMS analysis on an Apache logfile for a single date
GeoUsage log analyze </path/to/apache_logfile> --service-type=OGC:WMS --endpoint=/ows --time=2018-01-26

# perform WMS analysis on an Apache logfile for a date range
GeoUsage log analyze </path/to/apache_logfile> --service-type=OGC:WMS --endpoint=/ows --time=2018-01-26/2018-01-27

# perform WMS analysis on an Apache logfile for a single datetime
GeoUsage log analyze </path/to/apache_logfile> --service-type=OGC:WMS --endpoint=/ows --time=2018-01-26T11:11:11

# perform WMS analysis on an Apache logfile for a datetime range
GeoUsage log analyze </path/to/apache_logfile> --service-type=OGC:WMS --endpoint=/ows --time=2018-01-26T11:11:11/2018-01-27T12:32:11

# resolve IP addresses
GeoUsage log analyze </path/to/apache_logfile> --service-type=OGC:WMS --endpoint=/ows --verbosity=INFO --resolve-ips

# show top 10 unique IPs and top 10 layers
GeoUsage log analyze </path/to/apache_logfile> --service-type=OGC:WMS --endpoint=/ows --verbosity=INFO --resolve-ips --top=10

# add verbose mode
GeoUsage log analyze </path/to/apache_logfile> --service-type=OGC:WMS --endpoint=/ows --verbosity=INFO

# query a Mailman mailing list member count
GeoUsage mailing_list member_count
```

### Example

Analyse OGC:WPS service using nginx log files:
```
GeoUsage log analyze /var/log/nginx/access.log* --service-type=OGC:WPS --endpoint=/wps
```

### Using the API

```python
from GeoUsage.mailman import MailmanAdmin

ma = MailmanAdmin('http://example.org/mailman/admin/list', 'secret')
print(ma.member_count)
```

## Development

### Running Tests

```bash
# install dev requirements
pip install -r requirements-dev.txt

# run tests like this:
python3 GeoUsage/tests/run_tests.py

# or this:
python3 setup.py test

# measure code coverage
coverage run --source=GeoUsage -m unittest GeoUsage.tests.run_tests
coverage report -m
```

## Releasing

```bash
python3 setup.py sdist bdist_wheel --universal
twine upload dist/*
```

### Code Conventions

* [PEP8](https://www.python.org/dev/peps/pep-0008)

### Bugs and Issues

All bugs, enhancements and issues are managed on [GitHub](https://github.com/geopython/GeoUsage/issues).

## Contact

* [Tom Kralidis](https://github.com/tomkralidis)
