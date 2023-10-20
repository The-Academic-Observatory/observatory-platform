# Contributing
Welcome to the contributing guide for the [observatory-platform](https://github.com/The-Academic-Observatory/observatory-platform)!
We welcome contributions to the project, please see below for details about how to contribute.

## Contents
1. [Python:](#1-python) version and code style guidelines.
1. [Documentation:](#2-documentation) docstrings and readthedocs.org documentation.
1. [Unit Tests:](#3-unit-tests) how to write unit tests and where to store data.
1. [License and Copyright:](#4-license-and-copyright) what licenses are ok and not ok and how to use the automatic license checker.
1. [Development Workflow:](#5-development-workflow) how to develop a new feature.
1. [Deployment:](#6-deployment-maintainers-only) how to deploy the package to PyPI.

## 1. Python
The Observatory Platform is written in [Python](https://www.python.org/).

### 1.1. Version
A minimum version of Python 3.10 is required.

### 1.2. Code Style
The code style should conform to the Python [PEP 8 Style Guide for Python Code](https://www.python.org/dev/peps/pep-0008/).
Additional code style guidelines specific to this project include:
* Function parameters and return types should be annotated with [type hints](https://www.python.org/dev/peps/pep-0484/)
to make the API definition as clear as possible to end users of the library. The Python documentation page 
[typing — Support for type hints](https://docs.python.org/3/library/typing.html) provides a good starting point for 
learning how to use type hints.
* A maximum line length of 120 characters.
* Formatting with [Black](https://black.readthedocs.io/en/stable/).

## 2. Documentation

### 2.1. Python Docstrings
Python docstrings should be written for all classes, methods and functions using the Sphinx docstring format. See the 
Sphinx tutorial [Writing docstrings](https://sphinx-rtd-tutorial.readthedocs.io/en/latest/docstrings.html) for more 
details. Additional documentation style guidelines specific to this project include:

The instructions below show how to setup automatic docstring template generation with popular IDEs:
* Visual Studio Code: install the [autoDocstring](https://marketplace.visualstudio.com/items?itemName=njpwerner.autodocstring)
plugin. To enable docstrings to be generated in the Sphinx format, click Code > Preferences > Settings > Extensions >
AutoDocstring Settings and from the `Docstring Format` dropdown, choose `sphinx`.
* PyCharm: PyCharm supports docstring generation out of the box, however, the Sphinx docstring format may need to be
explicitly specified. Click PyCharm > Preferences > Tools > Python Integrated Tools and under the Docstrings heading,
choose `reStructedText` from the `Docstring format` dropdown.

### 2.2. Read the Docs documentation
The Observatory Platform documentation hosted at [observatory-platform.readthedocs.io](https://observatory-platform.readthedocs.io/en/latest/)
is built from the files in the `docs` directory.

An overview of the technologies used to build the documentation:
* Generated with [Sphinx](https://www.sphinx-doc.org/en/stable/).
* Theme: [sphinx-rtd-theme](https://github.com/readthedocs/sphinx_rtd_theme).
* Documentation can be written in both Markdown and ReStructuredText text. Markdown is preferred and is parsed with
[recommonmark](https://github.com/readthedocs/recommonmark).
* API reference documentation is automatically generated from docstrings with [sphinx-autoapi](https://github.com/readthedocs/sphinx-autoapi).

The Read the Docs documentation is built automatically on every pull request and push.

### 2.3. Building Documentation Locally
Make sure that the Observatory Platform is installed with the docs requirements:
```bash
pip install -e .[docs]
```

Navigate to the `docs` directory:
```bash
cd docs
```

Build the documentation with the following command:
```bash
make html
```

The documentation should be generated in the `docs/_build` directory. You can open the file `docs_/build/index.html`
in a browser to preview what the documentation will look like.

## 3. Unit Tests
Unit tests are written with the [Python unittest framework](https://docs.python.org/3/library/unittest.html).

The below code snippet is an example of a simple unit test. Create a class, in this example `TestString`, which
represents a batch of tests to implement (for instance all of the tests for a class). Implement tests for each
function as a new method beginning with `test_`, so that the Python unittest framework can find the methods
to test. For instance, the code snippet below tests the concatenation functionality for the Python String class in 
the function `test_concatenate`. See the [Python unittest framework](https://docs.python.org/3/library/unittest.html)
documentation for more details.

```python
import unittest


class TestString(unittest.TestCase):

    def test_concatenate(self):
        expected = "hello world"
        actual = "hello" + " " + "world"
        self.assertEqual(expected, actual)
```

### 3.1. Unit test location
* Test datasets should be kept in the `observatory-platform/fixtures` folder, which are stored in [Git LFS](https://git-lfs.github.com/).
* The Python unittest framework looks for files named test*.py, so make sure to put "test_" at the start of your test
filename. For example, the unit tests for the file `gc_utils.py` are contained in the file called `test_gc_utils.py`.

#### 3.1.1 Observatory Platform
The unit tests should be kept in the folder called `tests` at the root level of the project. The `tests` directory
mimics the folder structure of the `observatory_platform` Python package folder. For example, as illustrated in the
figure below, the tests for the code in the file `observatory-platform/observatory_platform/utils/gc_utils.py` are 
contained in the file `observatory-platform/tests/observatory_platform/utils/test_gc_utils.py`. 


An example of project and test directory structure:
```bash
|-- observatory-platform
    |-- .github
    |-- observatory_platform
        |-- scripts
        |-- telescopes
        |-- utils
            |-- __init__.py
            |-- gc_utils.py
            ...
        |-- __init__.py
    |-- tests
        |-- observatory_platform
            |-- scripts
            |-- telescopes
            |-- utils
                |-- __init__.py
                |-- test_gc_utils.py
                ...
            |-- __init__.py
        |-- data
        |-- __init__.py
    |-- .gitignore
    |-- CONTRIBUTING.md
    ...
```

#### 3.1.2 Dependent Repositories
Unit tests for the Observatory Platform's dependent repositories ([oaebu-workflows](https://github.com/The-Academic-Observatory/oaebu-workflows), [academic-observatory-workflows](https://github.com/The-Academic-Observatory/academic-observatory-workflows)) are stored differently. Tests for any code should be stored in a directory named `tests` which shares a directory with the code it is testing. For example, as illustrated in thefigure below, the tests for the code in the file `oaebu-workflows/oaebu_workflows/telescopes/oapen_metadata_telescpe.py` are contained in the file `oaebu-workflows/oaebu_workflows/telescopes/tests/test_oapen_metadata_telescpe.py`.

An example of project and test directory structure:
```bash
|-- oaebu-workflows
    |-- .github
    |-- .gitignore
    |-- oaebu_workflows
        |-- onix.py
        |-- telescopes
            |-- __init__.py
            |-- oapen_metadata_telescope.py
            |-- tests
            |-- __init__.py
                |-- test_oapen_metadata_telescope.py
        |-- tests
            |-- __init__.py
            |-- test_onix.py
        |-- fixtures
            |-- oapen_metadata
                |-- test_data.json
            |-- onix
                |-- test_data.json
    |-- CONTRIBUTING.md

```

### 3.2. Testing code that makes HTTP requests
To test code that makes HTTP requests:
* [VCR.py](https://vcrpy.readthedocs.io/en/latest/) is used to test code that makes HTTP requests, enabling the tests 
to work offline without calling the real endpoints. VCR.py records  the HTTP requests made by a section of code and 
stores the results in a file called a 'cassette'. When the same section of code is run again, the HTTP requests are read 
from the cassette, rather than calling the real endpoint.
* Cassettes should be committed into the folder `observatory-platform/fixtures/cassettes`.

### 3.3. Running unit tests
To run the unittests from the command line, execute the following command from the root of the project, it should
automatically discover all the unit tests:
```bash
python -m unittest
```

How to enable popular IDEs to run the unittests:
* PyCharm: PyCharm supports test discovery and execution for the Python unittest framework. You may need to configure
PyCharm to use the unittest framework. Click PyCharm > Preferences > Tools > Python Integrated Tools and under the 
Testing heading, choose `Unittests` from the `Default test runner` dropdown.
* VSCode: VSCode supports test discovery and execution for the Python unittest framework. Under the *Testing* panel, configure the tests to use the unittest framework and search for tests beginning with *test_**. 
  
A `.env` file will also need to be configured with the following variables set:
* GOOGLE_APPLICATION_CREDENTIALS - The path to your Google Cloud Project credentials
* TEST_GCP_PROJECT_ID - Your GCP Project ID
* TEST_GCP_DATA_LOCATION - The location of your GCP Project
* TEST_GCP_BUCKET_NAME - The name of your GCP testing bucket 

Some tests may also require access to Amazon Web Services. For these tests, the following additional envrionment variables are required:
* AWS_ACCESS_KEY_ID - Your AWS secret key ID
* AWS_SECRET_ACCESS_KEY - Your AWS secret key
* AWS_DEFAULT_REGION - The AWS region

## 4. License and Copyright
Contributors agree to release their source code under the Apache 2.0 license. Contributors retain their copyright.

Make sure to place the license header (below) at the top of each Python file, customising the year, copyright owner 
and author fields.

Typically, if you create the contribution as a part of your work at a company (including a University), you should
put the copyright owner as the company and not yourself. You may put yourself as the author. If you created the 
contribution independently and you own the copyright to the contribution, then you should place yourself as the 
copyright owner as well as the author.

```python
# Copyright [yyyy] [name of copyright owner]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Author: [name of author]
```

### 4.1. Dependencies
You **may** depend on a third party package if the third party package has a license from the
[`unencumbered`](https://opensource.google/docs/thirdparty/licenses/#unencumbered), 
[`permissive`](https://opensource.google/docs/thirdparty/licenses/#permissive),
[`notice`](https://opensource.google/docs/thirdparty/licenses/#notice),
[`reciprocal`](https://opensource.google/docs/thirdparty/licenses/#reciprocal) lists, or if the license is LGPL.

You **must not** depend on any package that has a license in the 
[`restricted`](https://opensource.google/docs/thirdparty/licenses/#restricted) or 
[`banned`](https://opensource.google/docs/thirdparty/licenses/#banned) license lists (unless the license is LGPL). 
Common examples of these licenses include, Creative Commons "Attribution-ShareAlike" (CC BY-SA), 
Creative Commons "Attribution-NoDerivs" (CC BY-ND), the GNU GPL and the A-GPL. These licenses are incompatible with the 
Apache 2.0 license that the Observatory Platform is released with. 

### 4.2. Dependency license checker
The licenses of the dependencies in `requirements.txt` are checked with [liccheck](https://pypi.org/project/liccheck/)
on each push and pull request as a part of the [Python package](https://github.com/The-Academic-Observatory/observatory-platform/actions?query=workflow%3A%22Python+package%22) Github Action.
* The list of authorized and unauthorized licenses are documented in [strategy.ini](https://github.com/The-Academic-Observatory/observatory-platform/blob/develop/strategy.ini)
* The `Python package` Github Action will fail if any unknown or unauthorized licenses are detected.
* If the license checker fails: 
  * Check the license of the dependency.
  * If the dependency has a license that may be depended on, add the exact license text, in lower case, under 
  `authorized_licenses` in [strategy.ini](https://github.com/The-Academic-Observatory/observatory-platform/blob/develop/strategy.ini).
  * If liccheck could not find license information for a package, and you can manually verify that the license is OK
  to be depended on, then add the package and version under the section `[Authorized Packages]` in [strategy.ini](https://github.com/The-Academic-Observatory/observatory-platform/blob/develop/strategy.ini)
  along with a comment about what the license is and a URL to the license.
  * If the dependency has a license that must not be depended on, then don't use that dependency.

### 4.3. Third Party Code
Code written by third parties (not the contributor) can be directly integrated into the project, although it is best
avoided if possible. The requirements for integrating third party code includes:
* The code requires one of the open source license from the [`unencumbered`](https://opensource.google/docs/thirdparty/licenses/#unencumbered), 
[`permissive`](https://opensource.google/docs/thirdparty/licenses/#permissive) or 
[`notice`](https://opensource.google/docs/thirdparty/licenses/#notice) lists.
* The third party open source code should be contained in its own file and not copied into other files. You may modify
the code in the file and make changes.
* The license for the third party open source code must be specified at the top of the file and in the 
LICENSES_THIRD_PARTY file.

You **must not** include any code from a project with a license in the 
[`restricted`](https://opensource.google/docs/thirdparty/licenses/#restricted) or 
[`banned`](https://opensource.google/docs/thirdparty/licenses/#banned) license lists published by Google. 
Common examples of these licenses include, Creative Commons "Attribution-ShareAlike" (CC BY-SA), 
Creative Commons "Attribution-NoDerivs" (CC BY-ND), the GNU GPL and the A-GPL. These licenses are incompatible with the 
Apache 2.0 license that the Observatory Platform is released with. 

## 5. Development Workflow
This section explains the development workflow used in the Observatory Platform project and its dependent repositories.

### 5.1. Branches
The [observatory-platform](https://github.com/The-Academic-Observatory/observatory-platform) and its dependent repositories each have only one long-lived branch - `main`. This branch is in continuous development; all official releases are made from a point in time of the `main` branch. The branching strategy employed is not unlike the popular [GitHub Flow](https://docs.github.com/en/get-started/quickstart/github-flow) and [trunk-based development](https://trunkbaseddevelopment.com/) strategies whereby all feature branches are created from and merged into `main`.

### 5.2. Developing a feature
GitHub has and [official guide](https://docs.github.com/en/get-started/quickstart/contributing-to-projects) on how to contribute to open-source projects when you do not have direct access to the repository.
 
If you do have direct repository access, the general workflow for working on a feature is as follows:

1. Clone the project locally.
2. Create a feature branch, branching off the `main` branch. The branch name should be descriptive of its changes.
3. Once your feature is ready and before making your pull request, make sure to [rebase](https://help.github.com/en/github/using-git/about-git-rebase) your changes onto the latest `origin/main` commit. It is a good idea to rebase regularly. It is preferred that bloated commits are squashed using the interactive tag when rebasing. 
4. Make your pull request:
    * Tag at least one reviewer in the pull request, so that they receive a notification and know to review it.
    * The guide on [How to write the perfect pull request](https://github.blog/2015-01-21-how-to-write-the-perfect-pull-request/) might be helpful.

Detailed instructions on how to use Git to accomplish this process are given below.

#### 1) Clone the project
Clone the Observatory Platform Github project, with either HTTPS:
```bash
git clone https://github.com/The-Academic-Observatory/observatory-platform.git
```

Or SSH (you need to setup an SSH keypair on Github for this to work):
```bash
git clone git@github.com:The-Academic-Observatory/observatory-platform.git
```

#### 2) Create a feature branch
Checkout main:
```bash
git checkout main
```

Then, create a new feature branch from main:
```bash
git checkout -b <your-feature-name>
``` 

#### 3) Rebase

##### 3.1) Sync main
Before rebasing, make sure that you have the latest changes from the origin main branch.

Fetch the latest changes from origin:
```bash
git fetch --all
```

Checkout the local main branch:
```bash
git checkout main
```

Merge the changes from upstream main onto your local branch:
```bash
git merge upstream/main
```

Git should just need to fast forward the changes, because we make our changes from feature branches.

##### 3.2) Rebase feature branch
Checkout your feature branch:
```bash
git checkout <your-feature-name>
```

Rebase your feature branch (the branch currently checked out) onto main.
```bash
git rebase -i main
```

## 6. Deployment (Maintainers Only)
This section contains information about how to deploy the project to PyPI.

### 6.1 Deploy to PyPI
Follow the instructions below to deploy the package to [PyPI](https://pypi.org/).

#### 6.1.1. Update setup.py and release on Github
In `setup.py` update `version` and `download_url` with the latest version number and the latest Github release download 
URL respectively:
```python
setup(
    version='19.12.0',
    download_url=('https://github.com/The-Academic-Observatory/observatory-platform/v19.12.0.tar.gz'
)
```

Commit these changes, push and make a new release on Github.

#### 6.1.2. Build the package
Enter the package folder:
```bash
cd observatory-platform
```

Ensure any dependencies are installed:
```bash
pip3 install -r requirements.txt
```

Create a source distribution for the package:
```bash
python3 setup.py sdist
```

#### 6.1.3. Upload to PyPI
Install twine, which we will use to upload the release to [PyPI](https://pypi.org/):
```bash
pip3 install twine
```

Use twine to upload the release to [PyPI](https://pypi.org/):
```bash
twine upload dist/*
```

