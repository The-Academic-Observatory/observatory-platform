# Contributing
Welcome to the contributing guide for the [academic-observatory](https://github.com/The-Academic-Observatory/academic-observatory)!

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
* Do not specify types using the Sphinx `:type [ParamName]: ...` syntax because we use Python type hints. 
The Sphinx extension, [sphinx-autodoc-typehints](https://github.com/agronholm/sphinx-autodoc-typehints),
is used to generate typing documentation based on Python 3 type hints.

The instructions below show how to setup automatic docstring template generation with popular IDEs:
* Visual Studio Code: install the [autoDocstring](https://marketplace.visualstudio.com/items?itemName=njpwerner.autodocstring)
plugin. To enable docstrings to be generated in the Sphinx format, click Code > Preferences > Settings > Extensions >
AutoDocstring Settings and from the `Docstring Format` dropdown, choose `sphinx`.
* PyCharm: PyCharm supports docstring generation out of the box, however, the Sphinx docstring format may need to be
explicitly specified. Click PyCharm > Preferences > Tools > Python Integrated Tools and under the Docstrings heading,
choose `reStructedText` from the `Docstring format` dropdown.

### Dependencies
Ensure that any Python dependencies have been added to `requirements.txt` including a minimum version number for each
dependency.

### Copyright
Place a copyright header at the top of each Python file, customising the year and the author. The following is a 
template for code written by Curtin University employees.

```python
#
# Copyright <year> Curtin University. All rights reserved.
#
# Author: <your name>
#
```

#### Third party code
Code written by third parties can be integrated into the project as long as:
* It has an explicit open source license.
* The open source license is in the [`notice`](https://opensource.google/docs/thirdparty/licenses/#notice), 
[`permissive`](https://opensource.google/docs/thirdparty/licenses/#permissive) or 
[`unencumbered`](https://opensource.google/docs/thirdparty/licenses/#unencumbered) lists published by Google.
* The third party open source code is contained in its own file and not copied into other files
* The license for the third party open source code must be specified at the top of the file and in the 
LICENSES_THIRD_PARTY file.

The [Google Open Source Licenses](https://opensource.google/docs/thirdparty/licenses/#) documentation explains why
the above guidelines are necessary.

#### Dependencies
Open source dependencies must also have a license in the [`notice`](https://opensource.google/docs/thirdparty/licenses/#notice), 
[`permissive`](https://opensource.google/docs/thirdparty/licenses/#permissive) or 
[`unencumbered`](https://opensource.google/docs/thirdparty/licenses/#unencumbered) lists published by Google.

The [Google Open Source Licenses](https://opensource.google/docs/thirdparty/licenses/#) documentation explains why
the above guidelines are necessary.

## Development Workflow
This section explains the development workflow used in the academic-observatory project.

### Branches
The [academic-observatory](https://github.com/The-Academic-Observatory/academic-observatory) project has two main 
branches `master` and `develop`. The `master` branch contains the most up to date version of 
the code base running in production whilst the `develop` branch contains code that is ready to be delivered 
in the next release. The article [A successful Git branching model](https://nvie.com/posts/a-successful-git-branching-model/)
provides a much richer overview.

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

Detailed instructions on how to use Git are given below.

#### Syncing your fork
Make sure your are in the working directory of your project.

Fetch the latest changes from upstream:
```bash
git fetch upstream
```

Checkout the local develop branch:
```bash
git checkout develop
```

Merge the changes from upstream develop onto your local branch:
```bash
git merge upstream/develop
```

Git should just need to fast forward the changes, because we make our changes from feature branches.

See more details on the [Syncing a fork](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/syncing-a-fork) 
Github page. 

#### Creating a feature branch
Make sure that you have synced your fork.

From the working directory of your project, checkout develop:
```bash
git checkout develop
```

Then, create a new feature branch from develop:
```bash
git checkout -b <your-feature-name>
``` 

#### Rebasing
Make sure that you have synced your fork.

Checkout your feature branch:
```bash
git checkout <your-feature-name>
```

Rebase your feature branch (the branch currently checked out) onto develop.
```bash
git rebase -i develop
```

## Deployment (Maintainers Only)
Follow the instructions below to deploy the package to [PyPI](https://pypi.org/).

### 1. Update setup.py and release on Github
In `setup.py` update `version` and `download_url` with the latest version number and the latest Github release download 
URL respectively:
```python
setup(
    version='19.12.0',
    download_url=('https://github.com/The-Academic-Observatory/academic-observatory/v19.12.0.tar.gz'
)
```

Commit these changes, push and make a new release on Github.

### 2. Build the package
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

### 3. Upload to PyPI
Install twine, which we will use to upload the release to [PyPI](https://pypi.org/):
```bash
pip3 install twine
```

Use twine to upload the release to [PyPI](https://pypi.org/):
```bash
twine upload dist/*
```