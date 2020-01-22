# academic-observatory
The `academic-observatory` is a Python API and a set of command line tools for downloading and processing data to 
understand how well universities operate as open knowledge institutions. 

## Installation
To install from [PyPI](https://pypi.org/) with pip:
```bash
pip3 install academic-observatory
```

To install from source with pip:
```bash
pip3 install -e .
```

## Quickstart

### GRID
To download all historical [Global Research Identifier Database (GRID)](https://grid.ac/) releases:
```bash
ao_util grid download
```

To create the GRID index:
```bash
ao_util grid index
```