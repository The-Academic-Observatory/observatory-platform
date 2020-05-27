# Installation
academic-observatory supports Python 3.7 and above on MacOS and Linux.

Dependencies:
* Python 3.7
* pip
* virtualenv
* Docker Engine or Docker Desktop.

See below for more details.

## System dependencies: MacOS
Install [Homebrew](https://brew.sh/) with the following command:

```bash
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

Install Python 3.7 with brew:
```bash
brew install python3.7
```

Install Docker Desktop by following the [Install Docker Desktop on Mac](https://docs.docker.com/docker-for-mac/install/) 
tutorial.

## System dependencies: Ubuntu 18.04
Update packages list and install software-properties-common:

```bash
sudo apt update
sudo apt install software-properties-common
```

Add deadsnakes PPA which contains Python 3.7 for Ubuntu 18.04; press `Enter` when prompted:
```bash
sudo add-apt-repository ppa:deadsnakes/ppa
```

Install Python 3.7 and Python 3.7 dev:
```bash
sudo apt install python3-distutils python3-setuptools python3-pip python3.7 python3.7-dev
```

Install Docker Engine by following the [Install Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/) tutorial.

## Installing from PyPI with pip
To install from [PyPI](https://pypi.org/) with `pip`:
```bash
pip3 install academic-observatory
```

## Installing from source with virtualenv
Clone the project:
```bash
git clone https://github.com/The-Academic-Observatory/academic-observatory
```

Enter the `academic-observatory` directory:
```bash
cd academic-observatory
```

Checkout the develop branch:
```
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
```

Install dependencies:
```bash
pip install -r requirements.txt
```

Install the package:
```bash
pip3 install -e .
```