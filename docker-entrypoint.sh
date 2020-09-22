#!/bin/bash -ex

echo "Running in $PWD"

chown $UID:$GID $DATA_DIR/*

whoami

sudo -Eu $USER_NAME "$@"
