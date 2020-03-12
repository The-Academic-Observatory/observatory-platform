# Contributing
Welcome to the contributing guide for the [academic-observatory](https://github.com/The-Academic-Observatory/academic-observatory)!
We welcome contributions to the project, please see below for details about how to contribute.

## Python
The academic-observatory is written in [Python](https://www.python.org/).

### Version
A minimum version of Python 3.7 is required.

### Code Style
The code style should conform to the Python [PEP 8 Style Guide for Python Code](https://www.python.org/dev/peps/pep-0008/).
Additional code style guidelines specific to this project include:
* Function parameters and return types should be annotated with [type hints](https://www.python.org/dev/peps/pep-0484/)
to make the API definition as clear as possible to end users of the library. The Python documentation page 
[typing — Support for type hints](https://docs.python.org/3/library/typing.html) provides a good starting point for 
learning how to use type hints.
* A maximum line length of 120 characters.

A number of IDEs provide support for automatically formatting code so that it confirms to the PEP 8 specification. 
Guides for popular Python IDEs are listed below.
* Visual Studio Code with the [Microsoft Python extension for Visual Studio Code](https://marketplace.visualstudio.com/items?itemName=ms-python.python): see [Linting Python in Visual Studio Code](https://code.visualstudio.com/docs/python/linting).
* PyCharm: see [Reformat and rearrange code](https://www.jetbrains.com/help/pycharm/reformat-and-rearrange-code.html).

### Documentation
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

### Unit Tests
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

Where to put the unit tests:
* The unit tests should be kept in the folder called `tests` at the root level of the project. The `tests` directory
mimics the folder structure of the `academic_observatory` Python package folder. For example, as illustrated in the
figure below, the tests for the code in the file `academic-observatory/academic_observatory/utils/ao_utils.py` are 
contained in the file `academic-observatory/tests/academic_observatory/utils/test_ao_utils.py`. 
* Small test datasets can be kept in the `academic-observatory/tests/data` folder, however, if the datasets are large 
they should be stored outside of git as it will make git operations such as cloning slow.
* The Python unittest framework looks for files named test*.py, so make sure to put "test_" at the start of your test
filename. For example, the unit tests for the file `ao_utils.py` are contained in the file called `test_ao_utils.py`.

An example of project and test directory structure:
```bash
|-- academic-observatory
    |-- .github
    |-- academic_observatory
        |-- scripts
        |-- telescopes
        |-- utils
            |-- __init__.py
            |-- ao_utils.py
            ...
        |-- __init__.py
    |-- tests
        |-- academic_observatory
            |-- scripts
            |-- telescopes
            |-- utils
                |-- __init__.py
                |-- test_ao_utils.py
                ...
            |-- __init__.py
        |-- data
        |-- __init__.py
    |-- .gitignore
    |-- CONTRIBUTING.md
    ...
```

To run the unittests from the command line, execute the following command from the root of the project, it should
automatically discover all of the unit tests:
```bash
python -m unittest
```

How to enable popular IDEs to run the unittests:
* PyCharm: PyCharm supports test discovery and execution for the Python unittest framework. You may need to configure
PyCharm to use the unittest framework. Click PyCharm > Preferences > Tools > Python Integrated Tools and under the 
Testing heading, choose `Unittests` from the `Default test runner` dropdown.

## License and Copyright
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

### Dependencies
You **may** depend on a third party package if the third party package has a license from the
[`unencumbered`](https://opensource.google/docs/thirdparty/licenses/#unencumbered), 
[`permissive`](https://opensource.google/docs/thirdparty/licenses/#permissive),
[`notice`](https://opensource.google/docs/thirdparty/licenses/#notice),
[`reciprocal`](https://opensource.google/docs/thirdparty/licenses/#reciprocal) lists published by Google.

You **must not** depend on any package that has a license in the 
[`restricted`](https://opensource.google/docs/thirdparty/licenses/#restricted) or 
[`banned`](https://opensource.google/docs/thirdparty/licenses/#banned) license lists published by Google. 
Common examples of these licenses include, Creative Commons "Attribution-ShareAlike" (CC BY-SA), 
Creative Commons "Attribution-NoDerivs" (CC BY-ND), the GNU GPL and the A-GPL. These licenses are incompatible with the 
Apache 2.0 license that the Academic Observatory is released with. 

### Third Party Code
Code written by third parties (not the contributor) can be directly integrated into the project, although it is best
avoided if possible. The requirements for integrating third party code includes:
* The code requires one of the open source license from the [`unencumbered`](https://opensource.google/docs/thirdparty/licenses/#unencumbered), 
[`permissive`](https://opensource.google/docs/thirdparty/licenses/#permissive) or 
[`notice`](https://opensource.google/docs/thirdparty/licenses/#notice) lists published by Google.
* The third party open source code should be contained in its own file and not copied into other files. You may modify
the code in the file and make changes.
* The license for the third party open source code must be specified at the top of the file and in the 
LICENSES_THIRD_PARTY file.

You **must not** include any code from a project with a license in the 
[`restricted`](https://opensource.google/docs/thirdparty/licenses/#restricted) or 
[`banned`](https://opensource.google/docs/thirdparty/licenses/#banned) license lists published by Google. 
Common examples of these licenses include, Creative Commons "Attribution-ShareAlike" (CC BY-SA), 
Creative Commons "Attribution-NoDerivs" (CC BY-ND), the GNU GPL and the A-GPL. These licenses are incompatible with the 
Apache 2.0 license that the Academic Observatory is released with. 

## Development Workflow
This section explains the development workflow used in the academic-observatory project.

### Branches
The [academic-observatory](https://github.com/The-Academic-Observatory/academic-observatory) project has two main 
branches `master` and `develop`. The `master` branch contains the most up to date version of 
the code base running in production whilst the `develop` branch contains code that is ready to be delivered 
in the next release. The article [A successful Git branching model](https://nvie.com/posts/a-successful-git-branching-model/)
provides a much richer overview of this approach.

### Developing a feature
The general workflow for working on a feature is as follows:
1. Fork the [academic-observatory](https://github.com/The-Academic-Observatory/academic-observatory) project onto your
own Github profile.
2. Create a feature branch in your fork, branching off the `develop` branch.
3. Implement your feature.
4. Once your feature is ready, before making your pull request, make sure to [rebase](https://help.github.com/en/github/using-git/about-git-rebase) 
your changes onto the latest `origin/develop` commit. 
5. Make your pull request.
* Tag a reviewer in the pull request, so that they receive a notification and know to review it.
* The guide on [How to write the perfect pull request](https://github.blog/2015-01-21-how-to-write-the-perfect-pull-request/)
might be helpful.

Detailed instructions on how to use Git to accomplish this process are given below.

#### 1) Clone the project
Fork the Academic Observatory project on Github.

Clone your fork, with either HTTPS:
```bash
git clone https://github.com/[your Github username]/academic-observatory.git
```

Or SSH (you need to setup an SSH keypair on Github for this to work):
```bash
git clone git@github.com:[your Github username]/academic-observatory.git
```

#### 2) Configure upstream
Make sure your are in the working directory of the project:
```bash
cd academic-observatory
```

Add the main Academic Observatory repository as upstream with either HTTPS:
```bash
git remote add upstream https://github.com/The-Academic-Observatory/academic-observatory.git
```

Or SSH:
```bash
git remote add upstream git@github.com:The-Academic-Observatory/academic-observatory.git
```

#### 3) Sync your fork
Fetch the latest changes from origin and upstream:
```bash
git fetch --all
```

Checkout the local develop branch:
```bash
git checkout -b develop origin/develop
```

Merge the changes from upstream develop onto your local branch:
```bash
git merge upstream/develop
```

Git should just need to fast forward the changes, because we make our changes from feature branches.

See more details on the [Syncing a fork](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/syncing-a-fork) Github page. 

#### 4) Create a feature branch
Checkout develop:
```bash
git checkout -b develop origin/develop
```

Then, create a new feature branch from develop:
```bash
git checkout -b <your-feature-name>
``` 

#### 5) Rebase
Checkout your feature branch:
```bash
git checkout <your-feature-name>
```

Rebase your feature branch (the branch currently checked out) onto develop.
```bash
git rebase -i develop
```

## Deployment (Maintainers Only)
This section contains information about how to deploy the project to PyPI and how to 
deploy updated documentation to Read the Docs (coming).

### Deploy to PyPI
Follow the instructions below to deploy the package to [PyPI](https://pypi.org/).

#### 1) Update setup.py and release on Github
In `setup.py` update `version` and `download_url` with the latest version number and the latest Github release download 
URL respectively:
```python
setup(
    version='19.12.0',
    download_url=('https://github.com/The-Academic-Observatory/academic-observatory/v19.12.0.tar.gz'
)
```

Commit these changes, push and make a new release on Github.

#### 2) Build the package
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

#### 3) Upload to PyPI
Install twine, which we will use to upload the release to [PyPI](https://pypi.org/):
```bash
pip3 install twine
```

Use twine to upload the release to [PyPI](https://pypi.org/):
```bash
twine upload dist/*
```

### Build and Deploy Documentation
This section explains how to build and deploy the documentation, which is contained in the `docs` directory.

Overview of the documentation:
* Generated with [Sphinx](https://www.sphinx-doc.org/en/stable/).
* Theme: [sphinx-rtd-theme](https://github.com/readthedocs/sphinx_rtd_theme).
* Documentation can be written in both Markdown and ReStructuredText text. Markdown is preferred and is parsed with
[recommonmark](https://github.com/readthedocs/recommonmark).
* API reference documentation is automatically generated from docstrings with [sphinx-autoapi](https://github.com/readthedocs/sphinx-autoapi).

#### Building documentation
Navigate to the `docs` directory:
```bash
cd docs
```

Install the requirements for building the documentation:
```bash
pip install -r requirements.txt
```

Build the documentation with the following command:
```bash
make html
```

The documentation should be generated in the `docs/_build` directory. You can open the file `docs_/build/index.html`
in a browser to preview what the documentation will look like.

#### Deploy with Read the Docs
Coming soon.