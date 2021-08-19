#!/bin/bash

# Defines

source observatory-platform/.env

venv_observatory_platform="venv"

## Functions

function lower_case() {
    echo $(echo $1 | tr '[:upper:]' '[:lower:]')
}

function ask_question () {
    local question="$1"
    shift
    local default_option="$1"
    shift
    local options=("$@")

    while :
    do
        local response
        read -p "$question" response
        response=${response:-$default_option}
        response=$(lower_case $response)

        for option in "${options[@]}"
        do
            if [ "$response" = "$option" ]; then
                break 2
            fi
        done
    done

    echo "$response"
}

function check_system() {
    if [ "$os" != "linux" ] && [ "$os" != "darwin" ]; then
        echo "Incompatible operating system detected: $os_human"
        exit 1
    fi

    if [ "$arch" != "x86_64" ] && [ "$arch" != "arm64" ]; then
        echo "Incompatible architecture detected: $arch"
        exit 1
    fi
}

function ask_install_observatory_tests() {
    question="Do you wish to install extra developer testing packages? [Y/n]: "
    options=("y" "n")
    default_option="y"
    install_test_extras=$(ask_question "$question" "$default_option" "${options[@]}")
}


function ask_install_observatory_api() {
    question="Do you wish to install the observatory-api? [Y/n]: "
    options=("y" "n")
    default_option="y"
    install_oapi=$(ask_question "$question" "$default_option" "${options[@]}")
}

function ask_install_observatory_dags() {
    question="Do you wish to install the observatory-dags? [Y/n]: "
    options=("y" "n")
    default_option="y"
    export install_odags=$(ask_question "$question" "$default_option" "${options[@]}")
}

function ask_config_type() {
    question="Do you want to use a local observatory platform configuration or use Terraform? (local, terraform) [local]: "
    options=("local" "terraform")
    default_option="local"
    config_type=$(ask_question "$question" "$default_option" "${options[@]}")
}

function ask_config_observatory_base() {

        question="Do you want to configure Observatory platform basic settings during config file generation? Note that if you do not configure it now, you need to configure all the sections tagged [Required] later on. [Y/n]: "
        options=("y" "n")
        default_option="y"
        config_observatory_base=$(ask_question "$question" "$default_option" "${options[@]}")
}

function configure_install_options() {
    echo "================================"
    echo "Configuring installation options"
    echo "================================"

    while :
    do
        ask_install_observatory_tests
        ask_install_observatory_api
        ask_install_observatory_dags
        ask_config_type
        ask_config_observatory_base

        echo -e "\n"

        echo "=========================================================="
        echo -e "Installation configuration summary:"
        echo "----------------------------------------------------------"
        echo "Operating system: $os_human, architecture: $arch"
        echo "Observatory platform version: $observatory_version"
        echo "Install Observatory Platform: y"
        echo "Install extra developer testing packages: $install_test_extras"
        echo "Install Observatory API: $install_oapi"
        echo "Install Observatory DAGs: $install_odags"
        echo "Observatory config type: $config_type"
        echo "Configure settings during config file generation step: $config_observatory_base"
        echo ""

        echo "Third party installs:"
        echo "Docker Compose: $docker_compose_version"

        if [ "$config_type" = "terraform" ]; then
        echo "Terraform: $terraform_version"
        echo "Packer: $packer_version"
        echo "Google Cloud SDK: $google_cloud_sdk_version"
        fi
        echo "=========================================================="
        echo -e "\n"

        local correct=""
        while [ "$correct" != "y" ] && [ "$correct" != "n" ]
        do
            read -p "Are these options correct? [Y/n]: " correct
            correct=${correct:-Y}
            correct=$(lower_case $correct)
        done

        if [ "$correct" = "y" ]; then
            break
        fi

        echo "Asking configuration questions again.  If you wish to exit the installation script, press Ctrl+C"
    done
}

function install_google_cloud_sdk() {
    echo "==========================="
    echo "Installing Google Cloud SDK"
    echo "==========================="

    local url="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${google_cloud_sdk_version}-${os}-${gcloud_sdk_arch}.tar.gz"
    sudo curl -L $url -o /usr/local/bin/google-cloud-sdk.tar.gz
    sudo rm -rf /usr/local/bin/google-cloud-sdk
    sudo tar -xzvf /usr/local/bin/google-cloud-sdk.tar.gz -C /usr/local/bin
    sudo rm /usr/local/bin/google-cloud-sdk.tar.gz
    sudo chmod +x /usr/local/bin/google-cloud-sdk
    sudo /usr/local/bin/google-cloud-sdk/install.sh
}

function install_terraform_deps() {
    echo "================================="
    echo "Installing Terraform dependencies"
    echo "================================="

    echo "Installing Packer"
    local url="https://releases.hashicorp.com/packer/${packer_version}/packer_${packer_version}_${os}_${terraform_arch}.zip"
    sudo rm -f /usr/local/bin/packer
    sudo curl -L $url -o /usr/local/bin/packer.zip
    sudo unzip /usr/local/bin/packer.zip -d /usr/local/bin/
    sudo chmod +x /usr/local/bin/packer
    sudo rm /usr/local/bin/packer.zip

    # Install Google Cloud SDK
    install_google_cloud_sdk

    echo "Installing Terraform"
    local url="https://releases.hashicorp.com/terraform/${terraform_version}/terraform_${terraform_version}_${os}_${terraform_arch}.zip"
    sudo curl -L $url -o /usr/local/bin/terraform.zip
    # When asked to replace, answer 'y'
    sudo rm -f /usr/local/bin/terraform
    sudo unzip /usr/local/bin/terraform.zip -d /usr/local/bin/
    sudo chmod +x /usr/local/bin/terraform
    sudo rm /usr/local/bin/terraform.zip
}

function install_ubuntu_system_deps() {
    echo "====================================="
    echo "Installing Ubuntu system dependencies"
    echo "====================================="

    sudo apt update
    sudo apt-get install -y software-properties-common curl git python3.8 python3.8-dev python3-pip

    echo "Installing Python dependencies"
    pip3 install -U virtualenv pbr

    echo "Installing docker"
    sudo apt-get install -y docker.io

    echo "Installing docker-compose"
    local url="https://github.com/docker/compose/releases/download/${docker_compose_version}/docker-compose-${os}-${arch}"
    sudo curl -L $url -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose

    echo "Adding $(id -nu) to the docker group"
    sudo usermod -aG docker $(id -nu)

    if [ "$config_type" = "terraform" ]; then
        install_terraform_deps
    fi

    echo -e "\n"
}

function install_macos_system_deps() {
    echo "====================================="
    echo "Installing Mac OS system dependencies"
    echo "====================================="

    if [ "$(command -v brew)" = "" ]; then
        echo "Installing Homebrew"
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
    fi

    echo "Installing Python 3.8"
    brew install python@3.8
    echo 'export PATH="/usr/local/opt/python@3.8/bin:$PATH"' >> ~/.bash_profile

    echo "Installing Python dependencies"
    pip3.8 install -U virtualenv pbr

    echo "Installing Docker"
    brew cask install docker

    if [ "$config_type" = "terraform" ]; then
        install_terraform_deps
    fi
}

function install_system_deps() {
    if [ "$os" = "linux" ]; then
        install_ubuntu_system_deps
    elif [ "$os" = "darwin" ]; then
        install_macos_system_deps
    fi

    # Need to wait for system deps to be installed before we can guarantee this works.
    export observatory_version=$(python3 -c "from pbr.version import VersionInfo; print(VersionInfo('observatory').release_string())")

}

function install_observatory_platform() {
    echo "Creating a Python virtual environment for installing the observatory platform"
    python3 -m venv $venv_observatory_platform

    echo "Activating the virtual environment for the observatory platform"
    source $venv_observatory_platform/bin/activate

    echo "Updating the virtual environment's Python packages"
    pip3 install -U pip virtualenv wheel

    echo "==========================================="
    echo "Installing the observatory-platform package"
    echo "==========================================="
    if [ "$install_test_extras" = "y" ]; then
        pip install -e observatory-platform[tests]
    else
        pip install -e observatory-platform
    fi

    echo -e "Deactivating observatory platform virtual environment"
    deactivate
    echo -e "\n"
}

function install_observatory_api() {
    if [ "$install_oapi" = "n" ]; then
        return 0
    fi

    echo "Activating the virtual environment for the observatory platform"
    source $venv_observatory_platform/bin/activate

    echo "=========================="
    echo "Installing Observatory API"
    echo "=========================="
    pip3 install -e observatory-api

    echo -e "Deactivating observatory platform virtual environment"
    deactivate
    echo -e "\n"
}

function install_observatory_dags() {
    if [ "$install_odags" = "n" ]; then
        return 0
    fi

    if [ "$install_oapi" = "n" ]; then
        echo "ERROR: You must install the Observatory API if installing the Observatory DAGs."
        exit 1
    fi

    echo "Activating the virtual environment for the observatory platform"
    source $venv_observatory_platform/bin/activate

    echo "==========================="
    echo "Installing Observatory DAGs"
    echo "==========================="
    pip3 install -e observatory-dags

    echo -e "Deactivating observatory platform virtual environment"
    deactivate
    echo -e "\n"
}

function generate_observatory_config() {
    echo "Activating the virtual environment for the observatory platform"
    source $venv_observatory_platform/bin/activate

    local interactive=""

    if [ "$config_observatory_base" = "y" ]; then
        interactive="--interactive"
    fi

    echo "============================="
    echo "Generating Observatory config"
    echo "============================="

    if [ "$config_type" = "local" ]; then
       observatory generate config local $interactive
    elif [ "$config_type" = "terraform" ]; then
        observatory generate config terraform $interactive
    fi

    echo -e "Deactivating observatory platform virtual environment"
    deactivate
    echo -e "\n"
}

#### ENTRY POINT ####

echo "=================================================================================================================================="
echo "Installing Academic Observatory Platform. You may be prompted at some stages to enter in a password to install system dependencies"
echo "=================================================================================================================================="
check_system
install_system_deps
configure_install_options
install_observatory_platform
install_observatory_api
install_observatory_dags
generate_observatory_config

echo "=================================================================================================================================="
echo "Installation complete."
echo "Please restart your computer for the docker installation to take effect."
echo -e "You can start the observatory platform after the restart by first activating the Python virtual environment with:\n  source $(pwd)/$venv_observatory_platform/bin/activate"
echo -e "Once activated, you can start the observatory with:\n  observatory platform start"