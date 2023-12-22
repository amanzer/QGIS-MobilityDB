# QGIS-MobilityDB
The aim of this research project is to explore ways of visualizing MobilityDB temporal geometries inside QGIS with its temporal controller.

# Tools

PostgreSQL : [https://www.postgresql.org/]
PostGIS : [https://postgis.net/]
MobilityDB : [https://github.com/MobilityDB/MobilityDB]
QGIS : [https://qgis.org/en/site/]
PyMeos : [https://pymeos.readthedocs.io/en/latest/]



# Installation 

The experimentation is conducted on Windows 11 with the help of WSL2 virtual machine of Ubuntu 22.



```bash
# in windows powershell install Ubuntu
wsl --install

```

After creating a user, to install mobilityDB and postgresql :

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

# MobilityDB

git clone https://github.com/MobilityDB/MobilityDB
mkdir MobilityDB/build
cd MobilityDB/build
cmake ..
make
sudo make install

```

To check if installation is working correctly :


```bash

sudo -i -u postgres
createdb mobility
psql -d mobility
CREATE EXTENSION POSTGIS;
CREATE EXTENSION MOBILITYDB;

```


Installation of QGIS, Move plugin, setup of osgeo4w, python packages(PyMeos, Psycopg2)

