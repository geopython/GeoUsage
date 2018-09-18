# GeoUsage
[![Build Status](https://travis-ci.org/geopython/GeoUsage.png)](https://travis-ci.org/geopython/GeoUsage)
[![Coverage Status](https://coveralls.io/repos/github/geopython/GeoUsage/badge.svg?branch=master)](https://coveralls.io/github/geopython/GeoUsage?branch=master)

Metrics Analysis for OGC Web Services

## Overview

GeoUsage is a pure Python package providing OGC Web Services usage analysis.

## Installation

The easiest way to install GeoUsage is via the Python [pip](https://pip.pypa.io/en/stable/)
utility:

```bash
pip install GeoUsage
```

### Requirements
- Python 3.  Works with Python 2.7
- [virtualenv](https://virtualenv.pypa.io/)

### Dependencies
Dependencies are listed in [requirements.txt](requirements.txt). Dependencies
are automatically installed during GeoUsage installation.

### Installing GeoUsage

```bash
# setup virtualenv
virtualenv --system-site-packages -p python3 GeoUsage
cd GeoUsage
source bin/activate

# clone codebase and install
git clone https://github.com/geopython/GeoUsage.git
cd GeoUsage
python setup.py build
python setup.py install
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
GeoUsage log analyze --service-type=OGC:WMS --logfile </path/to/apache_logfile>

# perform WMS analysis on an Apache logfile on a specific WMS endpoint
GeoUsage log analyze --service-type=OGC:WMS --endpoint=/ows --logfile </path/to/apache_logfile>

# perform WMS analysis on an Apache logfile for a single date
GeoUsage log analyze --service-type=OGC:WMS --endpoint=/ows --logfile </path/to/apache_logfile> --time=2018-01-26

# perform WMS analysis on an Apache logfile for a date range
GeoUsage log analyze --service-type=OGC:WMS --endpoint=/ows --logfile </path/to/apache_logfile> --time=2018-01-26/2018-01-27

# perform WMS analysis on an Apache logfile for a single datetime
GeoUsage log analyze --service-type=OGC:WMS --endpoint=/ows --logfile </path/to/apache_logfile> --time=2018-01-26T11:11:11

# perform WMS analysis on an Apache logfile for a datetime range
GeoUsage log analyze --service-type=OGC:WMS --endpoint=/ows --logfile </path/to/apache_logfile> --time=2018-01-26T11:11:11/2018-01-27T12:32:11

# resolve IP addresses
GeoUsage log analyze --service-type=OGC:WMS --endpoint=/ows --logfile </path/to/apache_logfile> --verbosity=INFO --resolve-ips

# add verbose mode
GeoUsage log analyze --service-type=OGC:WMS --endpoint=/ows --logfile </path/to/apache_logfile> --verbosity=INFO

# query a Mailman mailing list member count
GeoUsage mailing_list member_count
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
python GeoUsage/tests/run_tests.py

# or this:
python setup.py test

# measure code coverage
coverage run --source=GeoUsage -m unittest GeoUsage.tests.run_tests
coverage report -m
```

## Releasing

```bash
python setup.py sdist bdist_wheel --universal
twine upload dist/*
```

### Code Conventions

* [PEP8](https://www.python.org/dev/peps/pep-0008)

### Bugs and Issues

All bugs, enhancements and issues are managed on [GitHub](https://github.com/geopython/GeoUsage/issues).

## Contact

* [Tom Kralidis](https://github.com/tomkralidis)

