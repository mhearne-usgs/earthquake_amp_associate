#!/bin/bash

unamestr=`uname`
if [ "$unamestr" == 'Linux' ]; then
    prof=~/.bashrc
    mini_conda_url=https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
    matplotlibdir=~/.config/matplotlib
elif [ "$unamestr" == 'FreeBSD' ] || [ "$unamestr" == 'Darwin' ]; then
    prof=~/.bash_profile
    mini_conda_url=https://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh
    matplotlibdir=~/.matplotlib
else
    echo "Unsupported environment. Exiting."
    exit
fi

source $prof

# Name of virtual environment
VENV=associate_amps

py_ver=3.8

echo "Using python version $py_ver"


# Is conda installed?
conda --version
if [ $? -ne 0 ]; then
    echo "No conda detected, installing miniconda..."

    command -v curl >/dev/null 2>&1 || { echo >&2 "Script requires curl but it's not installed. Aborting."; exit 1; }

    curl -L $mini_conda_url -o miniconda.sh;

    # if curl fails, bow out gracefully
    if [ $? -ne 0 ];then
        echo "Failed to download miniconda installer shell script. Exiting."
        exit 1
    fi
    
    echo "Install directory: $HOME/miniconda"

    bash miniconda.sh -f -b -p $HOME/miniconda

    # if miniconda.sh fails, bow out gracefully
    if [ $? -ne 0 ];then
        echo "Failed to run miniconda installer shell script. Exiting."
        exit 1
    fi
    
    . $HOME/miniconda/etc/profile.d/conda.sh

    # remove the shell script
    rm miniconda.sh
else
    echo "conda detected, installing $VENV environment..."
fi

echo "Installing packages from conda-forge"

# Choose an environment file based on platform
# only add this line if it does not already exist
grep "/etc/profile.d/conda.sh" $prof
if [ $? -ne 0 ]; then
    echo ". $_CONDA_ROOT/etc/profile.d/conda.sh" >> $prof
fi


# Start in conda base environment
echo "Activate base virtual environment"
eval "$(conda shell.bash hook)"                                                
conda activate base

# Remove existing shakemap environment if it exists
conda remove -y -n $VENV --all

# Required package list:
package_list=(
      "python=$py_ver"
      "boto3"
      "ipython"
      "defusedxml"
      "pymysql"
      "pandas"
      "numpy"
      "pyyaml"
      "pytest"
      "pytest-cov"
      "sqlalchemy"
      "sqlalchemy-utils"
)

if [ $developer == 1 ]; then
    package_list=( "${package_list[@]}" "${dev_list[@]}" )
    echo ${package_list[*]}
fi

# Create a conda virtual environment
conda config --add channels 'conda-forge'
conda config --add channels 'defaults'
conda config --set channel_priority flexible

echo "Creating the $VENV virtual environment:"
conda create -y -n $VENV ${package_list[*]}

# Bail out at this point if the conda create command fails.
# Clean up zip files we've downloaded
if [ $? -ne 0 ]; then
    echo "Failed to create conda environment.  Resolve any conflicts, then try again."
    exit 1
fi


# Activate the new environment
echo "Activating the $VENV virtual environment"
conda activate $VENV

# if conda activate fails, bow out gracefully
if [ $? -ne 0 ];then
    echo "Failed to activate ${VENV} conda environment. Exiting."
    exit 1
fi

# upgrade pip, mostly so pip doesn't complain about not being new...
pip install --upgrade pip

# if pip upgrade fails, complain but try to keep going
if [ $? -ne 0 ];then
    echo "Failed to upgrade pip, trying to continue..."
    exit 1
fi

# The presence of a __pycache__ folder in bin/ can cause the pip
# install to fail... just to be safe, we'll delete it here.
if [ -d bin/__pycache__ ]; then
    rm -rf bin/__pycache__
fi


# This package
echo "Installing ${VENV}..."
pip install --no-deps -e .

# if pip install fails, bow out gracefully
if [ $? -ne 0 ];then
    echo "Failed to pip install this package. Exiting."
    exit 1
fi


# Tell the user they have to activate this environment
echo "Type 'conda activate $VENV' to use this new virtual environment."
