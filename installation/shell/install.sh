#!/bin/bash
#conda create -n py39 -y -c conda-forge python=3.9 pandas numpy
conda create -n py310 python=3.10 pandas numpy -y
source activate py310

mkdir ~/miniconda3/python_libs

python -m pip install --upgrade pip
export PYTHON_LIBS=~/miniconda3/python_libs

#export PYTHON_LIBS=~/python_libs

# Install TA LIB
cd $PYTHON_LIBS
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar xvfz ta-lib-0.4.0-src.tar.gz
rm ta-lib-0.4.0-src.tar.gz
cd ta-lib
./configure
make

# Install zipline-reloaded with source code
pip install zipline-reloaded # for dependecies
git clone git@github.com:stefan-jansen/zipline-reloaded.git
cd zipline-reloaded
python setup.py build_ext --inplace
python setup.py install

# Install TWS api
cd $PYTHON_LIBS
wget https://interactivebrokers.github.io/downloads/twsapi_macunix.1025.01.zip
unzip twsapi_macunix.1025.01.zip -d twsapi
rm twsapi_macunix.1025.01.zip
cd twsapi/IBJts/source/pythonclient
python setup.py install

# Install sharadar_db_bundle and its requirements
cd $PYTHON_LIBS
git clone https://github.com/Moccazio/sharadar_db_bundle
cd $PYTHON_LIBS/sharadar_db_bundle
pip install -r requirements.txt
pip install -r requirements.txt --upgrade --no-deps
python setup.py install

if [ "$?" -eq 0 ]
then
echo "INSTALLATION SUCCESSFUL"
else
echo "INSTALLATION FAILED"
source deactivate py310
conda remove --name py310 --all -y
fi

