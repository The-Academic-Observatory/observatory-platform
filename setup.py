from setuptools import setup, find_packages

long_description = '''
The Observatory Platform is an environment for fetching, processing and analysing data to understand how well 
universities operate as Open Knowledge Institutions. 

The Observatory Platform is built with Apache Airflow and includes DAGs (workflows) for processing: Crossref Metadata, 
Fundref, GRID, Microsoft Academic Graph (MAG) and Unpaywall.

See the Github Project here: https://github.com/The-Academic-Observatory/observatory-platform
See the documentation here: https://coki-academic-observatory.readthedocs-hosted.com/en/latest

The Observatory Platform is compatible with Python 3.7.
'''

with open('requirements.txt') as f:
    install_requires = f.read().splitlines()

with open('docs/requirements.txt') as f:
    docs_requires = f.read().splitlines()

setup(
    name='observatory-platform',
    version='20.7.0',
    description='Telescopes, Workflows and Data Services for the Observatory Platform',
    long_description=long_description,
    license='Apache License Version 2.0',
    author='Curtin University',
    author_email='agent@observatory.academy',
    url='https://github.com/The-Academic-Observatory/observatory-platform',
    packages=find_packages(),
    download_url='https://github.com/The-Academic-Observatory/observatory-platfor/v20.7.0.tar.gz',
    keywords=['science', 'data', 'workflows', 'academic institutes', 'observatory-platform'],
    install_requires=install_requires,
    extras_require={
        'docs': docs_requires,
        'tests': ['liccheck==0.4.*', 'flake8==3.8.*', 'coverage==5.2']
    },
    entry_points={
        'console_scripts': [
            # The new command line interface implemented with Click
            'observatory = observatory_platform.cli.observatory:cli'
        ]
    },
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities"
    ],
    python_requires='>=3.7'
)
