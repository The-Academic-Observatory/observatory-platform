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
cd academic-observatory
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

## Development

### Deployment
Follow the instructions below to deploy the package to [PyPI](https://pypi.org/).

#### Update setup.py and release on Github
In `setup.py` update `version` and `download_url` with the latest version number and the latest Github release download 
URL respectively:
```python
setup(
    version='19.12.0',
    download_url=('https://github.com/The-Academic-Observatory/academic-observatory/v19.12.0.tar.gz'
)
```

Commit these changes, push and make a new release on Github.

#### Build the package
Enter the package folder:
```bash
cd academic-observatory
```

Ensure any dependencies are installed:
```bash
pip3 install -r requirements.txt
```

Create a source distribution for the package:
```bash
python3 setup.py sdist
```

#### Upload to PyPI
Install twine, which we will use to upload the release to [PyPI](https://pypi.org/):
```bash
pip3 install twine
```

Use twine to upload the release to [PyPI](https://pypi.org/):
```bash
twine upload dist/*
```

