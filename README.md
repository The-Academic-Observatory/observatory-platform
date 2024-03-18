![Observatory Platform](https://raw.githubusercontent.com/The-Academic-Observatory/observatory-platform/develop/logo.jpg)

The Observatory Platform is a set of dependencies used by the Curtin Open Knowledge Initiative (COKI) for running its 
Airflow based workflows to fetch, process and analyse bibliometric datasets.

The workflows for the project can be seen in at:
* [Academic Observatory Workflows](https://github.com/The-Academic-Observatory/academic-observatory-workflows)
* [OAeBU Workflows](https://github.com/The-Academic-Observatory/oaebu-workflows)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Version](https://img.shields.io/badge/python-3.10-blue)](https://img.shields.io/badge/python-3.10-blue)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![Python package](https://github.com/The-Academic-Observatory/observatory-platform/workflows/Unit%20Tests/badge.svg)
[![Documentation Status](https://readthedocs.org/projects/observatory-platform/badge/?version=latest)](https://observatory-platform.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/The-Academic-Observatory/observatory-platform/branch/develop/graph/badge.svg)](https://codecov.io/gh/The-Academic-Observatory/observatory-platform)
[![DOI](https://zenodo.org/badge/227744539.svg)](https://zenodo.org/badge/latestdoi/227744539)

## Dependencies
Observatory Platform supports Python 3.10, Ubuntu Linux 22.04 and MacOS 10.14, on x86 architecture.

System dependencies:
* Python 3.10
* Pip
* virtualenv
* Google Cloud SDK (optional): https://cloud.google.com/sdk/docs/install-sdk

## Installation & Unit Tests
Enter observatory-platform folder:
```bash
cd observatory-platform
```

Install dependencies:
```bash
pip install -e .[tests] --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-no-providers-3.10.txt
```

Run unit tests:
```bash
python3 -m unittest discover -v
```

## Python Package Reference
See the Read the Docs website for documentation on the Python package [https://observatory-platform.readthedocs.io](https://observatory-platform.readthedocs.io)

## Dependent Repositories
The Observatory Platform is a dependency for other repositories developed and maintained by [The Academic Observatory](https://github.com/The-Academic-Observatory):
* [Academic Observatory Workflows](https://github.com/The-Academic-Observatory/academic-observatory-workflows)
* [OAeBU Workflows](https://github.com/The-Academic-Observatory/oaebu-workflows)
