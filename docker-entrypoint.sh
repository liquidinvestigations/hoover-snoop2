#!/bin/bash -ex

echo "Running in $PWD"

mkdir -p /opt/hoover/snoop/testsuite/volumes/snoop-pg/data
chown $UID:$GID -R /opt/hoover/snoop/testsuite/volumes/snoop-pg/data

chown -R 666:666 /opt/magic-definitions
chown -R 666:666 /opt/libpst

chown $UID:$GID $DATA_DIR/*

whoami

exec gosu $USER_NAME "$@"
#sudo -Eu $USER_NAME "$@"
#sudo -Eu $USER_NAME /wait && /runserver
