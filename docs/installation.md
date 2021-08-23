# Installation
Observatory Platform supports Python 3.8 on Linux and MacOS on x86-64 and ARM platforms.

The installer script supports installation on the following systems:
* Ubuntu 20.04 on x86-64
* Mac OS (insert version) on (platform).

System dependencies:
* Python 3.8
* Bash
* pip
* git
* virtualenv 20 or greater
* Docker Engine or Docker Desktop.

Currently we only support installation of the observatory platform from the source code repository. We may support installation through PyPI packages, and from Docker images in the future.

## Ubuntu Linux 20.04

Install git if you do not have it with:
```bash
sudo apt install git
```

Clone the observatory-platform:
```bash
git clone https://github.com/The-Academic-Observatory/observatory-platform
```

Run the observatory-platform installer script:
```bash
cd observatory-platform
./install.sh
```

## Mac OS

Clone the observatory-platform:
```bash
git clone https://github.com/The-Academic-Observatory/observatory-platform
```

If git is not installed, MacOS will prompt you with a window to install git first.

Run the observatory-platform installer script:
```bash
cd observatory-platform
./install.sh
```

## Installer script

The installer script might prompt you for your password at some point to install system dependencies.

You will be asked a series of questions during the installation process to customise your installation, and will be given a chance to configure your observatory platform settings.

If you selected the terraform install option, you may be prompted by the Google Cloud SDK to answer a couple of installation questions.  It is safe to use the default options, however, to make use of their cli tools, make sure you specify the correct path to the ```.bashrc``` file.  In Linux this is ```/home/youraccountusername/.bashrc```.  In Mac OS, this is ```/Users/youraccountusername/.bashrc```.

### Local install

The local install option will install the observatory platform to be able to be run on your local machine.

### Terraform install

The terraform install option will install the observatory platform to be able to be deployed in the cloud using terraform.

## Restarting after install

If you did not have Docker installed prior to running the installation script, you should restart the computer first for configuration changes to take effect. If Docker had previously been installed and configured, you can skip the restart.

## Using the observatory after installation

You can launch the observatory platform after installation by first activating the Python virtual environment, and then starting the platform:
```bash
cd observatory-platform
source venv/bin/activate
observatory platform start
```