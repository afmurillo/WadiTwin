#!/bin/bash

cwd=$(pwd)
version=$(lsb_release -rs )

# Wrong version warning
if [ "$version" != "20.04" ]
then
  printf "Warning! This installation script has only been tested on Ubuntu 20.04 LTS and will likely not work on your Ubuntu version.\n\n"
fi

doc=false
test=false

# Setting up test and doc parameters
while getopts ":dt" opt; do
  case $opt in
    d)
      printf "Installing with documentation dependencies.\n"
      doc=true
      ;;
    t)
      printf "Installing with testing dependencies.\n"
      test=true
      ;;
    \?)
      printf "Unknown option. Proceeding without installing documentation and testing dependencies.\n"
      ;;
  esac
done

sleep 3

# Update apt
sudo apt update

# Installing necessary packages
sudo apt install -y git python2 python3 python3-pip curl

# Get python2 pip
curl https://bootstrap.pypa.io/pip/2.7/get-pip.py --output get-pip.py
sudo python2 get-pip.py
rm get-pip.py

# CPPPO Correct Version 4.0.*
sudo pip install cpppo==4.0.*

# MiniCPS
cd ~
git clone --depth 1 https://github.com/afmurillo/minicps.git || git -C minicps pull
cd minicps
sudo python2 -m pip install .

# Installing other DHALSIM dependencies
sudo pip install pathlib==1.0.*
sudo pip install pyyaml==5.3.*
sudo pip install numpy==1.16.*

# Mininet from source
cd ~
git clone --depth 1 -b 2.3.0 https://github.com/mininet/mininet.git || git -C mininet pull
cd mininet
sudo PYTHON=python2 ./util/install.sh -fnv

# Installing testing pip dependencies
if [ "$test" = true ]
then
  sudo pip2 install netaddr==0.8.*
  sudo pip2 install flaky==3.7.*
  sudo pip2 install pytest==4.6.*
  sudo pip2 install pytest-timeout==1.4.*
  sudo pip2 install pytest-cov==2.12.*
  sudo pip2 install pytest-mock==3.6.*
fi

# Install DHALSIM
cd "${cwd}" || { printf "Failure: Could not find DHALSIM directory\n"; exit 1; }

# Install without doc and test
if [ "$test" = false ] && [ "$doc" = false ]
then
  sudo python3 -m pip install -e .

  printf "\nInstallation finished. You can now run DHALSIM by using \n\t<sudo dhalsim your_config.yaml>.\n"
  exit 0;
fi

# Install doc
if [ "$test" = false ]
then
  sudo python3 -m pip install -e .[doc]

  printf "\nInstallation finished. You can now run DHALSIM by using \n\t<sudo dhalsim your_config.yaml>.\n"
  exit 0;
fi

# Install test
if [ "$doc" = false ]
then
  sudo python3 -m pip install -e .[test]

  printf "\nInstallation finished. You can now run DHALSIM by using \n\t<sudo dhalsim your_config.yaml>.\n"
  exit 0;
fi

# Install test and doc
sudo python3 -m pip install -e .[test,doc]

printf "\nInstallation finished. You can now run DHALSIM by using \n\t<sudo dhalsim your_config.yaml>.\n"
exit 0;