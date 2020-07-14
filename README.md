![Academic Observatory](./Academic_Observatory_Logo.jpg?raw=true)

[![Documentation Status](https://readthedocs.com/projects/coki-academic-observatory/badge/?version=latest&token=bcb93984ae9a67950f7e61be1d6f35c975e0bb64cc1f688124fbf452b727c669)](https://coki-academic-observatory.readthedocs-hosted.com/en/latest/?badge=latest)

The `academic-observatory` is a Python API and a set of command line tools for downloading and processing data to 
understand how well universities operate as open knowledge institutions. 

## Installation
To install from [PyPI](https://pypi.org/) with pip:
```bash
pip3 install academic-observatory
```

### From source
Clone the project:
```bash
git clone https://github.com/The-Academic-Observatory/academic-observatory
```

Enter the academic-observatory directory:
```bash
cd academic-observatory
git checkout develop
```

Find the path to your Python 3.7 version, you will use it in the next step:
```bash
which python3.7
/usr/local/bin/python3.7
```

Create a virtual environment (replace the --python parameter with the value you got from the previous step):
```bash
virtualenv --python=/usr/local/bin/python3.7 --no-site-packages venv 
```

Activate the virtual environment:
```bash
source venv/bin/activate
````

Install dependencies:
```bash
pip install -r requirements.txt 
```

Install the package:
```bash
pip3 install -e .
```

## Quickstart
The following is a quickstart tutorial to get you started with the Academic Observatory command line tool.

### GRID
To download all historical [Global Research Identifier Database (GRID)](https://grid.ac/) releases:
```bash
aoutil grid download
```

To get help with the above command, including what other parameters are available, run:
```bash
aoutil grid download -h
```

To create the GRID index:
```bash
aoutil grid index
```

To get help with the above command, including what other parameters are available, run:
```bash
aoutil grid index -h
```

### OAI-PMH
Create a CSV file that has a column of potential OAI-PMH URLs and a header: 
```csv
url
http://digitalknowledge.cput.ac.za/oai/request
http://vital.seals.ac.za:8080/vital/oai/provider
http://dspace.nwu.ac.za/oai/request?verb=ListIdentifiers&metadataPrefix=oai_dc
http://eprints.ru.ac.za/cgi/oai2
http://uir.unisa.ac.za/oai/request
```

Supply this CSV to the following command, to fetch the given list of OAI-PMH endpoints:
```bash
aoutil oai_pmh fetch_endpoints --input oai_pmh_sources_small.csv --key url
```

To get help with this command type:
```bash
aoutil oai_pmh fetch_endpoints -h
```

After running the fetch_endpoints command, you can fetch OAI-PMH records within a given date range:
```bash
aoutil oai_pmh fetch_records --start_date 2019-11-01 --end_date 2019-11-10
```

To get help with the above command, including what other parameters are available, run:
```bash
aoutil oai_pmh fetch_records -h
```

### Common Crawl
To run the next command you will need:
* A table in BigQuery with the Common Crawl index and the GRID index joined and the institues you would like to get
the full text data for in the table.
* The environment variable GOOGLE_APPLICATION_CREDENTIALS needs to be set. See [here](https://cloud.google.com/docs/authentication/getting-started)
for more details.

To fetch the full text for a particular University, run the following, this example is for Curtin University.
```bash
aoutil common_crawl fetch_full_text --table_name coki-jamie-dev.grid_common_crawl.curtin_demo --start_date 2019-08 \
--end_date 2019-09
```

To get help with the above command, including what other parameters are available, run:
```bash
aoutil common_crawl fetch_full_text -h
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

