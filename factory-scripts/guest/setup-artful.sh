#!/bin/bash -ex

apt-get update -qq

# install basic stuff
apt-get install -yqq curl wget build-essential libmagic1 python3-dev virtualenv libxml2-dev libxslt1-dev openjdk-8-jre

# install development packages
apt-get install -yqq vim tmux htop man tcpdump

# install tools for archives, pst, msg, gpg
apt-get install -yqq p7zip-full p7zip-rar cpanminus pst-utils gnupg

# install postgresql
apt-get install -yqq postgresql libpq-dev postgresql-client

# wabbit
apt-get install -yqq rabbitmq-server

# turn off wabbit so the VM shuts down without timing out
systemctl stop rabbitmq-server

# msgconvert
cpanm --notest Email::Outlook::Message

# tika
mkdir /opt/tika
wget http://archive.apache.org/dist/tika/tika-server-1.17.jar -O /opt/tika/tika-server.jar

# download testdata
git clone https://github.com/hoover/testdata.git /opt/testdata

# set up snoop's virtualenv
virtualenv -p python3 /opt/snoop2-venv
/opt/snoop2-venv/bin/pip install -r /opt/snoop2/requirements.txt

# create snoop's database
sudo -u postgres createuser --superuser ubuntu
sudo -u postgres createdb -O ubuntu snoop2

# turn off postgresql so the VM shuts down without timing out
systemctl stop postgresql
