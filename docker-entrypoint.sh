#!/bin/bash -ex

echo "Running in $PWD"

chown $UID:$GID -R $DATA_DIR/blobs

whoami

sudo -Eu $USER_NAME "$@"
