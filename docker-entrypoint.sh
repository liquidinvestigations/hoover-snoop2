#!/bin/bash -ex

echo "Running in $PWD"

mkdir -p /opt/hoover/snoop/testsuite/volumes/snoop-pg/data
chown $UID:$GID -R /opt/hoover/snoop/testsuite/volumes/snoop-pg/data

mkdir -p /opt/hoover/snoop/static
chown $UID:$GID -R /opt/hoover/snoop/static

chown $UID:$GID $DATA_DIR/*

whoami

sudo -Eu $USER_NAME "$@"
