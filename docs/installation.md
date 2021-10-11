# Installation
Observatory Platform supports Python 3.8, Ubuntu Linux 20.04 and MacOS 10.14, on x86 architecture.  

## System dependencies
* Python 3.8
* Pip
* Docker
* virtualenv
* curl

Make sure you first have curl and bash installed on your system. MacOS comes with curl and bash. If you need to install curl on Ubuntu, run `sudo apt install -y curl`. Then run the following in a terminal:
```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/The-Academic-Observatory/observatory-platform/main/install.sh)"
chmod +x install.sh
./install.sh
```

The installer script will prompt you with a series of questions to customise your installation, and optionally configure the observatory.  At some point you might be asked to enter your user password in order to install system dependencies.  If you only want to run the observatory platform, then select the `pypi` installation type. If you want to modify or develop the platform, select the `source` installation type.

The script will create a Python virtual environment in the `observatory_venv` directory.

There are two types of observatory platform deployments. `local` and `terraform`. The `local` installation allows you to run the observatory platform on the locally installed machine. The `terraform` installation deploys the platform to the cloud. See the documentation section on Terraform deployment for more details.

You will also have the option of installing additional workflows. See the the GitHub pages for the [academic-observatory-workflows](https://github.com/The-Academic-Observatory/academic-observatory-workflows) and the [oaebu-workflows](https://github.com/The-Academic-Observatory/oaebu-workflows) for more information.

