#!/bin/bash

venv_observatory_platform="observatory_venv"
airflow_version="2.6.3"
python_version="3.8"

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

function ask_github_https_or_ssh() {
    options=("https" "ssh")
    options_str=$(echo ${options[@]} | sed "s/ /, /")
    default_option="https"
    question="Do you wish to use https or ssh to clone the source repository? ($options_str) [${default_option}]: "
    clone_mode=$(ask_question "$question" "$default_option" "${options[@]}")

    if [ "$clone_mode" = "https" ]; then
        clone_prefix="https://github.com/"
    else
        clone_prefix="git@github.com:"
    fi

}

function ask_install_mode() {
    options=("pypi" "source")
    options_str=$(echo ${options[@]} | sed "s/ /, /")
    default_option="pypi"
    question="Do you wish to install with pypi (pip) or from source? If you want to just run the observatory platform, pypi is recommended. If you intend to modify or develop the platform, source is recommended. ($options_str) [${default_option}]: "
    mode=$(ask_question "$question" "$default_option" "${options[@]}")

    if [ "$mode" = "source" ]; then
        pip_install_env_flag="-e"
        ask_github_https_or_ssh
    fi
}

function ask_install_observatory_tests() {
    options=("y" "n")
    default_option="y"
    options_str=$(echo ${options[@]} | sed "s/ /, /")
    question="Do you wish to install extra developer testing packages? ($options_str) [${default_option}]: "
    install_test_extras=$(ask_question "$question" "$default_option" "${options[@]}")

    if [ "$install_test_extras" = "y" ]; then
        test_suffix="[tests]"
    fi
}

function ask_install_academic_observatory_workflows() {
    options=("y" "n")
    default_option="y"
    options_str=$(echo ${options[@]} | sed "s/ /, /")
    question="Do you wish to install the academic-observatory-workflows? ($options_str) [${default_option}]: "
    export install_ao_workflows=$(ask_question "$question" "$default_option" "${options[@]}")
}

function ask_install_oaebu_workflows() {
    options=("y" "n")
    default_option="y"
    options_str=$(echo ${options[@]} | sed "s/ /, /")
    question="Do you wish to install the oaebu-workflows? ($options_str) [${default_option}]: "
    export install_oaebu_workflows=$(ask_question "$question" "$default_option" "${options[@]}")
}

function ask_config_type() {
    options=("local" "terraform")
    default_option="local"
    options_str=$(echo ${options[@]} | sed "s/ /, /")
    question="Do you want to use a local observatory platform configuration or use Terraform? ($options_str) [${default_option}]: "
    config_type=$(ask_question "$question" "$default_option" "${options[@]}")
}

function ask_config_observatory_base() {
    options=("y" "n")
    default_option="y"
    options_str=$(echo ${options[@]} | sed "s/ /, /")
    question="Do you want to configure Observatory platform basic settings during config file generation? Note that if you do not configure it now, you need to configure all the sections tagged [Required] later on by editing the config.yaml or config-terraform.yaml file. ($options_str) [${default_option}]: "
    config_observatory_base=$(ask_question "$question" "$default_option" "${options[@]}")
}

function ask_config_path_custom() {
    question="Path to save generated config file to [leave blank for default config path]: "
    default_option=""
    read -p "$question" config_path
    config_path=${config_path:-$default_option}
}

function configure_install_options() {
    echo "================================"
    echo "Configuring installation options"
    echo "================================"

    while :
    do
        # Configure options
        install_oapi="y"

        ask_install_mode
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
        echo "Install Observatory Platform: y"
        echo "Installation type: $mode"
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
            read -p "Are these options correct? (y, n) [y]: " correct
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
    echo "---------------------------"
    echo "Installing Google Cloud SDK"
    echo "---------------------------"

    local url="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${google_cloud_sdk_version}-${os}-${gcloud_sdk_arch}.tar.gz"
    sudo curl -L $url -o /usr/local/bin/google-cloud-sdk.tar.gz
    sudo rm -rf /usr/local/bin/google-cloud-sdk
    sudo tar -xzvf /usr/local/bin/google-cloud-sdk.tar.gz -C /usr/local/bin
    sudo rm /usr/local/bin/google-cloud-sdk.tar.gz
    sudo chmod +x /usr/local/bin/google-cloud-sdk
    sudo /usr/local/bin/google-cloud-sdk/install.sh
}

function install_terraform_deps() {
    ## Package versions to install

    terraform_version="1.0.5"
    packer_version="1.6.0"
    google_cloud_sdk_version="330.0.0"

    terraform_arch=$arch
    if [ "$arch" = "x86_64" ]; then
        terraform_arch="amd64"
    fi

    gcloud_sdk_arch=$arch
    if [ "$arch" = "arm64" ]; then
        gcloud_sdk_arch="arm"
    fi

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
    sudo apt-get install -y software-properties-common curl git python${python_version} python${python_version}-dev python3-pip python3-virtualenv

    echo "--------------------------"
    echo "Creating Python virtualenv"
    echo "--------------------------"

    virtualenv -p python${python_version} $venv_observatory_platform

    echo "-----------------"
    echo "Installing docker"
    echo "-----------------"

    sudo apt-get install -y docker.io docker-compose-plugin

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
    echo "Installing Python ${python_version}"
    echo "---------------------"

    brew install python@${python_version}
    echo 'export PATH="/usr/local/opt/python@${python_version}/bin:$PATH"' >> ~/.bash_profile

    echo "------------------------------"
    echo "Installing Python dependencies"
    echo "------------------------------"

    pip3 install -U virtualenv

    echo "--------------------------"
    echo "Creating Python virtualenv"
    echo "--------------------------"

    virtualenv -p /usr/local/opt/python@${python_version}/Frameworks/Python.framework/Versions/${python_version}/bin/python3 $venv_observatory_platform

    echo "-----------------"
    echo "Installing Docker"
    echo "-----------------"

    brew install --cask docker

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
}

function install_observatory_platform() {
    echo "--------------------------------------------------"
    echo "Updating the virtual environment's Python packages"
    echo "--------------------------------------------------"

    pip3 install -U pip virtualenv wheel

    echo "==========================================="
    echo "Installing the observatory-platform package"
    echo "==========================================="

    if [ "$mode" = "source" ]; then
        git clone ${clone_prefix}The-Academic-Observatory/observatory-platform.git
        cd observatory-platform
    fi

    pip3 install ${pip_install_env_flag} observatory-api${test_suffix} --constraint https://raw.githubusercontent.com/apache/airflow/constraints-${airflow_version}/constraints-no-providers-${python_version}.txt
    pip3 install ${pip_install_env_flag} observatory-platform${test_suffix} --constraint https://raw.githubusercontent.com/apache/airflow/constraints-${airflow_version}/constraints-no-providers-${python_version}.txt
}

function install_observatory_api() {
    if [ "$install_oapi" = "n" ]; then
        return 0
    fi

    echo "=========================="
    echo "Installing Observatory API"
    echo "=========================="



}

function install_academic_observatory_workflows() {
    if [ "$install_ao_workflows" = "n" ]; then
        return 0
    fi

    echo "========================================="
    echo "Installing academic observatory workflows"
    echo "========================================="

    local prefix=""

    if [ "$mode" = "source" ]; then
        mkdir -p workflows
        cd workflows
        git clone ${clone_prefix}The-Academic-Observatory/academic-observatory-workflows.git
        cd ..
        prefix="workflows/"
    fi

    pip3 install ${pip_install_env_flag} ${prefix}academic-observatory-workflows${test_suffix} --constraint https://raw.githubusercontent.com/apache/airflow/constraints-${airflow_version}/constraints-no-providers-${python_version}.txt
}

function install_oaebu_workflows() {
    if [ "$install_oaebu_workflows" = "n" ]; then
        return 0
    fi

    echo "=========================="
    echo "Installing oaebu workflows"
    echo "=========================="

    local prefix=""

    if [ "$mode" = "source" ]; then
        mkdir -p workflows
        cd workflows
        git clone ${clone_prefix}The-Academic-Observatory/oaebu-workflows.git
        cd ..
        prefix="workflows/"
    fi

    pip3 install ${pip_install_env_flag} ${prefix}oaebu-workflows${test_suffix} --constraint https://raw.githubusercontent.com/apache/airflow/constraints-${airflow_version}/constraints-no-providers-${python_version}.txt
}

function generate_observatory_config() {
    ask_config_path_custom

    local interactive=""
    local ao_wf=""
    local oaebu_wf=""
    local oapi=""
    local config_path_arg=""
    local editable=""

    if [ "$config_observatory_base" = "y" ]; then
        interactive="--interactive"
    fi

    if [ "$install_ao_workflows" = "y" ]; then
        ao_wf="--ao-wf"
    fi

    if [ "$install_oaebu_workflows" = "y" ]; then
        oaebu_wf="--oaebu-wf"
    fi

    if [ "$install_oapi" = "y" ]; then
        oapi="--oapi"
    fi

    if [ "$config_path" != "" ]; then
        config_path_arg="--config-path $config_path"
    fi

    if [ "$mode" = "source" ]; then
        editable="--editable"
    fi

    echo "============================="
    echo "Generating Observatory config"
    echo "============================="

    observatory generate config $config_path_arg $editable $interactive $ao_wf $oaebu_wf $oapi $config_type
}

#### ENTRY POINT ####

echo "=================================================================================================================================="
echo "Installing Academic Observatory Platform. You may be prompted at some stages to enter in a password to install system dependencies"
echo "=================================================================================================================================="

set_os_arch
check_system
configure_install_options
install_system_deps

source $venv_observatory_platform/bin/activate

install_observatory_platform
install_academic_observatory_workflows
install_oaebu_workflows

if [ "$mode" = "source" ]; then
    cd ..
fi

generate_observatory_config

deactivate

echo "=================================================================================================================================="
echo "Installation complete."
echo "Please restart your computer for the Docker installation to take effect."
echo -e "You can start the observatory platform after the restart by first activating the Python virtual environment with:\n  source ${PWD}/${venv_observatory_platform}/bin/activate"
echo -e "Once activated, you can start the observatory with: observatory platform start"