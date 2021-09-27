#!/bin/bash

# Defines

source installer_functions.sh


#### ENTRY POINT ####

echo "=================================================================================================================================="
echo "Installing Academic Observatory Platform. You may be prompted at some stages to enter in a password to install system dependencies"
echo "=================================================================================================================================="

set_os_arch
check_system
configure_install_options
install_system_deps
install_observatory_platform
install_observatory_api
install_academic_observatory_workflows

generate_observatory_config

echo "=================================================================================================================================="
echo "Installation complete."
echo "Please restart your computer for the docker installation to take effect."
echo -e "You can start the observatory platform after the restart by first activating the Python virtual environment with:\n  source $(pwd)/$venv_observatory_platform/bin/activate"
echo -e "Once activated, you can start the observatory with:\n  observatory platform start"