#!/bin/bash

venv_observatory_platform="venv"

function set_os_arch() {
    os=$(uname -s)
    os_human=$os
    os=$(lower_case $os)

    if [ "$os" == "Darwin" ]; then
        os_human="Mac OS $(uname -r)"
    fi

    arch=$(uname -m)
    arch=$(lower_case $arch)
}

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

function ask_install_academic_observatory_workflows() {
    question="Do you wish to install the academic-observatory-workflows? [Y/n]: "
    options=("y" "n")
    default_option="y"
    export install_ao_workflows=$(ask_question "$question" "$default_option" "${options[@]}")
}

function ask_install_oaebu_workflows() {
    question="Do you wish to install the oaebu-workflows? [Y/n]: "
    options=("y" "n")
    default_option="y"
    export install_oaebu_workflows=$(ask_question "$question" "$default_option" "${options[@]}")
}

function ask_config_type() {
    question="Do you want to use a local observatory platform configuration or use Terraform? (local, terraform) [local]: "
    options=("local" "terraform")
    default_option="local"
    config_type=$(ask_question "$question" "$default_option" "${options[@]}")
}

function ask_config_observatory_base() {

        question="Do you want to configure Observatory platform basic settings during config file generation? Note that if you do not configure it now, you need to configure all the sections tagged [Required] later on by editing the config.yaml or config-terraform.yaml file. [Y/n]: "
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
        # Configure options
        install_oapi="y"

        ask_install_observatory_tests
        ask_install_academic_observatory_workflows
        ask_install_oaebu_workflows
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
        echo "Install Academic Observatory Workflows: $install_ao_workflows"
        echo "Install OAEBU Workflows: $install_oaebu_workflows"
        echo "Observatory type: $config_type"
        echo "Configure settings during config file generation step: $config_observatory_base"
        echo ""
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

    echo "-----------------"
    echo "Installing Packer"
    echo "-----------------"

    local url="https://releases.hashicorp.com/packer/${packer_version}/packer_${packer_version}_${os}_${terraform_arch}.zip"
    sudo rm -f /usr/local/bin/packer
    sudo curl -L $url -o /usr/local/bin/packer.zip
    sudo unzip /usr/local/bin/packer.zip -d /usr/local/bin/
    sudo chmod +x /usr/local/bin/packer
    sudo rm /usr/local/bin/packer.zip

    # Install Google Cloud SDK
    install_google_cloud_sdk

    echo "--------------------"
    echo "Installing Terraform"
    echo "--------------------"

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

    echo "------------------------------"
    echo "Installing Python dependencies"
    echo "------------------------------"

    pip3 install -U virtualenv

    echo "--------------------------"
    echo "Creating Python virtualenv"
    echo "--------------------------"

    virtualenv -p python3.8 $venv_observatory_platform

    echo "-----------------"
    echo "Installing docker"
    echo "-----------------"

    sudo apt-get install -y docker.io

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

    echo "---------------------"
    echo "Installing Python 3.8"
    echo "---------------------"

    brew install python@3.8
    echo 'export PATH="/usr/local/opt/python@3.8/bin:$PATH"' >> ~/.bash_profile

    echo "------------------------------"
    echo "Installing Python dependencies"
    echo "------------------------------"

    pip3 install -U virtualenv

    echo "--------------------------"
    echo "Creating Python virtualenv"
    echo "--------------------------"

    virtualenv -p /usr/local/opt/python@3.8/Frameworks/Python.framework/Versions/3.8/bin/python3 $venv_observatory_platform

    echo "-----------------"
    echo "Installing Docker"
    echo "-----------------"

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
    export observatory_version=$(python3 -c "from importlib_metadata import metadata; print(metadata('observatory-platform').get('Version'))")

}

function install_observatory_platform() {
    echo "Activating the virtual environment for the observatory platform"
    source $venv_observatory_platform/bin/activate

    echo "--------------------------------------------------"
    echo "Updating the virtual environment's Python packages"
    echo "--------------------------------------------------"

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

function install_academic_observatory_workflows() {
    if [ "$install_ao_workflows" = "n" ]; then
        return 0
    fi

    echo "Activating the virtual environment for the observatory platform"
    source $venv_observatory_platform/bin/activate

    echo "=========================================="
    echo "Cloning the academic observatory workflows"
    echo "=========================================="

    mkdir -p workflows
    cd workflows
    git clone git@github.com:The-Academic-Observatory/academic-observatory-workflows.git
    cd ..

    echo "========================================="
    echo "Installing academic observatory workflows"
    echo "========================================="

    if [ ! -d "workflows/academic-observatory-workflows" ]; then
        mkdir -p workflows
        cd workflows
        git clone git@github.com:The-Academic-Observatory/oaebu-workflows.git
        cd ..
    fi

    if [ "$install_test_extras" = "y" ]; then
        pip3 install -e workflows/academic-observatory-workflows[tests]
    else
        pip3 install -e workflows/academic-observatory-workflows
    fi

    echo -e "Deactivating observatory platform virtual environment"
    deactivate
    echo -e "\n"
}

function install_academic_observatory_workflows() {
    if [ "$install_ao_workflows" = "n" ]; then
        return 0
    fi

    echo "Activating the virtual environment for the observatory platform"
    source $venv_observatory_platform/bin/activate

    echo "=========================="
    echo "Installing oaebu workflows"
    echo "=========================="

    if [ ! -d "workflows/oaebu-workflows" ]; then
        mkdir -p workflows
        cd workflows
        git clone git@github.com:The-Academic-Observatory/oaebu-workflows.git
        cd ..
    fi

    if [ "$install_test_extras" = "y" ]; then
        pip3 install -e workflows/oaebu-workflows[tests]
    else
        pip3 install -e workflows/oaebu-workflows
    fi

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