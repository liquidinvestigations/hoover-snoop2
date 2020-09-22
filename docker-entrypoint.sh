#!/bin/bash -ex

echo "Running in $PWD"

chown $UID:$GID -R $DATA_DIR
chown $UID:$GID -R $DATA_DIR/blobs
chown $UID:$GID -R $DATA_DIR/blobs/uploads
chown $UID:$GID -R $DATA_DIR/static

whoami

sudo -Eu $USER_NAME "$@"
