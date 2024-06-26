#!/bin/bash
#conda create -n py39 -y -c conda-forge python=3.9 pandas numpy
conda create -n py39 python=3.9 pandas numpy jupyterlab jupyter notebook logbook -y
source activate py39

# mkdir ~/python_libs

python -m pip install --upgrade pip
export PYTHON_LIBS=~/miniconda3/envs/py39/lib 

#export PYTHON_LIBS=~/python_libs

# Install TA LIB
cd $PYTHON_LIBS
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar xvfz ta-lib-0.4.0-src.tar.gz
rm ta-lib-0.4.0-src.tar.gz
cd ta-lib
./configure
make
sudo make install

# Install TWS api
cd $PYTHON_LIBS
wget https://interactivebrokers.github.io/downloads/twsapi_macunix.1025.01.zip
unzip twsapi_macunix.1025.01.zip -d twsapi
rm twsapi_macunix.1025.01.zip
cd twsapi/IBJts/source/pythonclient
pip install wheel
python setup.py sdist
python setup.py bdist_wheel
pip install --upgrade dist/ibapi-10.25.1-py3-none-any.whl

# Install sharadar_db_bundle and its requirements
cd $PYTHON_LIBS
git clone https://github.com/Moccazio/sharadar_db_bundle
#git clone  https://github.com/alphaville76/sharadar_db_bundle.git
cd $PYTHON_LIBS/sharadar_db_bundle
pip install -r requirements.txt
python setup.py sdist
python setup.py bdist_wheel
pip install --upgrade --force-reinstall dist/sharadar_db_bundle-2.0-py3-none-any.whl

if [ "$?" -eq 0 ]
then
echo "INSTALLATION SUCCESSFUL"
else
echo "INSTALLATION FAILED"
source deactivate py39
conda remove --name py39 --all -y
fi
