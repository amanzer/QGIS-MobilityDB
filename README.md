# QGIS-MobilityDB
The aim of this research project is to explore ways of visualizing MobilityDB temporal geometries inside QGIS with its temporal controller.

# Tools

PostgreSQL : [https://www.postgresql.org/]
PostGIS : [https://postgis.net/]
MobilityDB : [https://github.com/MobilityDB/MobilityDB]
QGIS : [https://qgis.org/en/site/]
Move: [https://github.com/mschoema/move]
PyMeos : [https://pymeos.readthedocs.io/en/latest/]



# Installation 

The experimentation is conducted on Windows 11 with the help of WSL2 virtual machine of Ubuntu 24 (Ubuntu-Preview on Microsoft store).
To install postgresql, MobilityDB and Meos :

```bash

# PostgreSQL 16

sudo sh -c 'echo "deb https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
sudo apt-get update
sudo apt-get -y install postgresql-16

# PostGIS

sudo apt-get install postgis

# Pre-requisites for MobilityDB

sudo apt install build-essential cmake postgresql-server-dev-16 libproj-dev libjson-c-dev libgsl-dev libgeos-dev

# MobilityDB and Meos

git clone https://github.com/MobilityDB/MobilityDB
mkdir MobilityDB/build
cd MobilityDB/build
cmake -DCMAKE_BUILD_TYPE=Debug -DMEOS=1 ..
make
sudo make install


```

After installing everything, update the postgresql.conf file with these changes :

```bash
# file location : /etc/postgresql/16/main/postgresql.conf

listen_addresses = '*'	# To interact with host windows os
...
shared_preload_libraries = 'postgis-3'
max_locks_per_transaction = 128

```


To check if installation is working correctly, you can test it like this :


```bash

sudo -i -u postgres
createdb mobility
psql -d mobility
CREATE EXTENSION POSTGIS;
CREATE EXTENSION MOBILITYDB;

```

Follow the instructions on these pages to install QGIS and its Move plugin:


- To install QGIS on wsl, follow the instruction to install on Ubuntu here : [https://www.qgis.org/fr/site/forusers/alldownloads.html]


- Install the Move plugin : [https://github.com/mschoema/move].


For this project, we use Python **3.11.7**, as of right now, with these packages :

```bash

pip install shapely
pip install pandas
pip install cffi

pip install --pre --force-reinstall --no-deps pymeos pymeos_cffi


# Note that in Ubuntu 23 and above, system python is treated as externally managed, to install these library to work within QGIS dev
# environment add this to pip :
--break-system-packages


```