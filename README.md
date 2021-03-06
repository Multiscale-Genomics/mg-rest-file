# mg-rest-file
Data retrieval of files or sections of files

[![Build Status](https://travis-ci.org/Multiscale-Genomics/mg-rest-file.svg?branch=master)](https://travis-ci.org/Multiscale-Genomics/mg-rest-file) [![Code Health](https://landscape.io/github/Multiscale-Genomics/mg-rest-file/master/landscape.svg?style=flat)](https://landscape.io/github/Multiscale-Genomics/mg-rest-file/master)

# Requirements
- Python 2.7.10+
- pyenv
- pyenv virtualenv
- Python Modules:
  - mg-dm-api
  - mg-rest-util
  - Flask
  - Flask-Restful
  - Waitress

# Installation
Cloneing from GitHub:
```
git clone https://github.com/Multiscale-Genomics/mg-rest-file.git
```
To get this to be picked up by pip if part of a webserver then:
```
pip install --editable .
pip install -r requirements.txt
```
This should install the required packages listed in the `setup.py` script.


Installation via pip:
```
pip install git+https://github.com/Multiscale-Genomics/mg-rest-file.git
```

# Configuration files
Requires a file with the name `mongodb.cnf` with the following parameters to define the MongoDB server:
```
[dmp]
host = localhost
port = 27017
user = testuser
pass = test123
db = dmp
```

Customise the `rest/auth_meta.json` file to locate the authentication server

# Setting up a server
```
git clone https://github.com/Multiscale-Genomics/mg-rest-file.git

cd mg-rest-dm
pyenv virtualenv 2.7.12 mg-rest-dm
pyenv activate mg-rest-dm
pip install git+https://github.com/Multiscale-Genomics/mg-dm-api.git
pip install -e .
pip deactivate
```
Starting the service:
```
nohup ${PATH_2_PYENV}/versions/2.7.12/envs/mg-rest-file/bin/waitress-serve --listen=127.0.0.1:5003 rest.app:app &
```
