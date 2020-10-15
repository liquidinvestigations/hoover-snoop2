#!/bin/bash -ex

echo "Running in $PWD"

mkdir -p /opt/hoover/snoop/testsuite/volumes/snoop-pg/data
chown $UID:$GID -R /opt/hoover/snoop/testsuite/volumes/snoop-pg/data

chown -R 666:666 /runserver
chown -R 666:666 /opt/magic-definitions
chown -R 666:666 /opt/libpst

sudo chown -R 666:666 /var/log/celery/
sudo chown -R 666:666 /var/run/celery/

chown $UID:$GID $DATA_DIR/*

whoami

echo $SNOOP_DB

exec gosu $USER_NAME "$@"
#sudo -Eu $USER_NAME "$@"
#sudo -Eu $USER_NAME /wait && /runserver
