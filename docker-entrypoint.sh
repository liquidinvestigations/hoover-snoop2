#!/bin/bash -ex

echo "Running in $PWD"

chown $UID:$GID $DATA_DIR/*
chown $UID:$GID ./volumes/snoop-pg/data

whoami

sudo -Eu $USER_NAME "$@"
