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

# supervisor
apt-get install -yqq supervisor

# turn off wabbit so the VM shuts down without timing out
systemctl stop rabbitmq-server

# msgconvert
cpanm --notest Email::Outlook::Message

# tika
mkdir /opt/tika
wget http://archive.apache.org/dist/tika/tika-server-1.17.jar -O /opt/tika/tika-server.jar

cat > /etc/supervisor/conf.d/tika.conf <<EOF
[program:tika]
user = ubuntu
command = java -jar /opt/tika/tika-server.jar
redirect_stderr = true
autostart = true
startsecs = 5
EOF
supervisorctl update

# download testdata
git clone https://github.com/liquidinvestigations/testdata.git /opt/testdata

# download magic file
wget https://github.com/liquidinvestigations/magic-definitions/raw/master/magic.mgc -O /opt/snoop2/magic.mgc

# set up snoop's virtualenv
virtualenv -p python3 /opt/snoop2-venv
/opt/snoop2-venv/bin/pip install -r /opt/snoop2/requirements.txt

# create snoop's database
sudo -u postgres createuser --superuser ubuntu
sudo -u postgres createdb -O ubuntu snoop2

# migrate databases
sudo -u ubuntu PYTHONDONTWRITEBYTECODE=yesgoddamnit /opt/snoop2-venv/bin/python /opt/snoop2/manage.py migrate

# collect static files
sudo -u ubuntu PYTHONDONTWRITEBYTECODE=yesgoddamnit /opt/snoop2-venv/bin/python /opt/snoop2/manage.py collectstatic --noinput

# create superuser
sudo -u ubuntu PYTHONDONTWRITEBYTECODE=yesgoddamnit /opt/snoop2-venv/bin/python /opt/snoop2/manage.py shell <<EOF
from django.contrib.auth.models import User
user = User.objects.create_user('admin', password='password')
user.is_superuser=True
user.is_staff=True
user.save()
EOF

# create testdata collection
sudo -u ubuntu PYTHONDONTWRITEBYTECODE=yesgoddamnit /opt/snoop2-venv/bin/python /opt/snoop2/manage.py createcollection testdata /opt/testdata/

# turn off postgresql so the VM shuts down without timing out
systemctl stop postgresql
