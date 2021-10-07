# Installation
Observatory Platform supports Python 3.8, Ubuntu Linux 20.04 and MacOS 10.14, on x86 architecture.  If you use the GitHub installation method, you only need to have `git` installed.  The installer script will fetch the system dependencies for you.  For other installation methods, e.g., `pip` you will want to make sure you have the system dependencies installed beforehand.

## System dependencies: Ubuntu 20.04
* Python 3.8 (`sudo apt install python3`)
* Pip (`sudo apt install python3-pip`)
* Docker (`sudo apt install docker.io`)
* virtualenv (`pip3 install virtualenv`)

## System dependencies: MacOS 10.14 or above
* Homebrew (`/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)`)
* Docker (`brew cask install docker`)
* Python 3.8 (`brew install python@3.8`)
* virtualenv (`pip3 install virtualenv`)

You should add your username to the docker group with
```
sudo usermod -aG docker $(id -nu)
```
and reboot the computer for Docker changes to take effect.

## Installation type

There are two types of installations. `local` and `terraform`. The `local` installation allows you to run the observatory platform on the locally installed machine. The `terraform` installation deploys the platform to the cloud. See the documentation section on Terraform deployment for more details.

## Installation via pip
Users who just want to use the observatory platform are encouraged to install the platform through pip. For developers who want to edit the observatory platform, install from source instead.

To install with pip, make sure you install the system dependencies first.  Once the system dependencies are installed, we recommend creating a Python virtual environment and installing the observatory in that, i.e.,

```bash
virtualenv venv  # Creates the virtual environment
source venv/bin/activate  # Enters the virtual environment
pip3 install observatory-platform  # Installs the observatory-platform package
```

The Academic Observatory also has several workflows you can install and use.  Currently, these are the `academic-observatory-workflows` and the `oaebu-workflows`.  To install these, run
```bash
pip3 install academic-observatory-workflows oaebu-workflows
```

See the the GitHub pages for the [academic-observatory-workflows](https://github.com/The-Academic-Observatory/academic-observatory-workflows) and the [oaebu-workflows](https://github.com/The-Academic-Observatory/oaebu-workflows) for more information.

If you want to install the test dependencies for the observatory, add the `[tests]` suffix to the pip modules, e.g.,
```bash
pip3 install observatory-platform[tests], academic-observatory-workflows[tests]
```

If you want to install the Terraform dependencies you can run
```
install_observatory_platform_terraform_deps
```

To exit the virtual environment run
```
deactivate
```

## Installation from source via GitHub repo
To install from the GitHub repository, make sure you have `git` installed (`sudo apt install git`).  This method is useful if you want to do development or make modifications to the observatory platform.


### Clone the repository
```bash
git clone https://github.com/The-Academic-Observatory/observatory-platform.git
```

### Run the installer script
Once you have cloned the repo, enter the project directory and run the installer script:
```bash
cd observatory-platform
./install_observatory_platform
```

You will be prompted by the installer script to customise the installation, e.g., test frameworks, academic-observatory-workflows.


## Configuring the Observatory Platform

If you have installed the observatory platform through the **GitHub repository**, the `install_observatory_platform` script will prompt you for options to configure the platform.  You can select to do this interactively by answering a bunch of questions, or for a default configuration to be generated, and to manually edit the configuration file at a later time.

If you have installed the observatory platform through `pip`, you can go through the same question and answer process as the GitHub installation method by running
```
configure_observatory_platform
```

Alternatively, you can directly use the observatory cli tool to generate configuration files.  See
```
observatory generate config --help
```
for more information.