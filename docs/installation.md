# Installation
Observatory Platform supports Python 3.7 and above on Linux and MacOS.

Dependencies:
* Python 3.7
* pip
* virtualenv 20 or greater
* Docker Engine or Docker Desktop.

See below for more details.

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

Install Python 3.7:
```bash
sudo apt install python3.7 python3.7-dev
```

Install pip:
```bash
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3.7 get-pip.py
```

Install virtualenv 20 or greater:
```
pip install --upgrade virtualenv
```

Install Docker Engine:
* Following the [Install Docker Engine on Ubuntu](https://docs.docker.com/engine/install/ubuntu/) tutorial.
* Make sure that Docker can be run without sudo, e.g. `sudo usermod -aG docker your-username`

## System dependencies: MacOS
Install [Homebrew](https://brew.sh/) with the following command:
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
```

Install Python 3.7 with brew:
```bash
brew install python@3.7
```

Add Python 3.7 to path:
```bash
echo 'export PATH="/usr/local/opt/python@3.7/bin:$PATH"' >> ~/.bash_profile
```

Install virtualenv 20 or greater:
```
pip3.7 install --upgrade virtualenv
```

Install Docker Desktop:
* Follow the [Install Docker Desktop on Mac](https://docs.docker.com/docker-for-mac/install/) tutorial.

## Installing the Observatory Platform
Make sure that you have followed the above instructions for installing the observatory-platform dependencies,
for your respective platform.

Clone the project:
```bash
git clone https://github.com/The-Academic-Observatory/observatory-platform
```

Enter the `observatory-platform` directory:
```bash
cd observatory-platform
```

Checkout the develop branch:
```
git checkout develop
```

Create a virtual environment:
```bash
virtualenv -p python3.7 venv
```

Activate the virtual environment:
```bash
source venv/bin/activate
```

Install the `observatory-platform` package:
```bash
pip3 install -e observatory-platform
```

Install the `observatory-api` package (optional):
```bash
pip3 install -e observatory-api
```

Install the `observatory-dags` package (optional):
```bash
pip3 install -e observatory-dags
```

Install the `observatory-reports` package (optional):
```bash
pip3 install -e observatory-reports
```