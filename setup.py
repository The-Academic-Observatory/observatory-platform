from setuptools import setup, find_packages

long_description = '''
academic-observatory is a Python API and a set of command line tools for downloading and processing data to 
understand how well universities operate as open knowledge institutions.

See the documentation here: https://github.com/The-Academic-Observatory/academic-observatory

academic-observatory is compatible with Python 3.7.
'''

with open('requirements.txt') as f:
    install_requires = f.read().splitlines()

setup(
    name='academic-observatory',
    version='20.7.0',
    description='Telescopes, Workflows and Data Services for the Academic Observatory.',
    long_description=long_description,
    license='Copyright 2019 Curtin University',
    author='Curtin University',
    author_email='agent@observatory.academy',
    url='https://github.com/The-Academic-Observatory/academic-observatory',
    packages=find_packages(),
    download_url='https://github.com/The-Academic-Observatory/academic-observatory/v19.12.0.tar.gz',
    keywords=['science', 'data', 'workflows', 'academic institutes', 'academic-observatory'],
    install_requires=install_requires,
    extras_require={
        'dev': [
            'Sphinx==2.4.*',
            'sphinx-rtd-theme==0.4.*',
            'sphinx-autoapi==1.2.*',
            'recommonmark==0.6.*']
    },
    entry_points={
        'console_scripts': [
            # The new command line interface implemented with Click
            'observatory = academic_observatory.cli.observatory:cli'
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
