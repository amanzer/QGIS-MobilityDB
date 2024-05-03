# QGIS-MobilityDB
The aim of this research project is to explore ways of visualizing MobilityDB temporal geometries inside QGIS with its temporal controller.

# Tools

PostgreSQL 16 : [https://www.postgresql.org/]
PostGIS 3.4.2 : [https://postgis.net/]
MobilityDB : [https://github.com/MobilityDB/MobilityDB]
QGIS 3.4: [https://qgis.org/en/site/]
Move : [https://github.com/mschoema/move]
PyMeos : [https://pymeos.readthedocs.io/en/latest/]



# Installation 

The experimentation is conducted on both Ubuntu 22.0(wsl2 VM) and Ubuntu 23.0


Installation steps for postgresql, MobilityDB:

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
# file location on wsl2 : /etc/postgresql/16/main/postgresql.conf

listen_addresses = '*'	# To interact with host windows os
...
shared_preload_libraries = 'postgis-3'
max_locks_per_transaction = 128

```


- Qgis installation is explained here : [https://www.qgis.org/fr/site/forusers/alldownloads.html]


- Follow the instructions here to install Move : [https://github.com/mschoema/move].

- Download and compile pymeos : https://github.com/MobilityDB/PyMEOS
 

--- 

Remarks :

```bash
# Note that in Ubuntu 23 and above, system python is treated as externally managed, to install these library to work within QGIS dev
# environment add this to pip :
--break-system-packages

```

To setup Github ssh and credentials on a new Ubuntu vm :


```bash

ssh-keygen -t rsa -b 4096 -C "email@gmail.com"

eval "$(ssh-agent -s)"

ssh-add ~/.ssh/id_rsa

cat ~/.ssh/id_rsa.pub
#Add key to Github website


git config --global user.name ""
git config --global user.email ""


```