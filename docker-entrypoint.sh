#!/bin/bash -ex

if [[ ! -d "$DATA_DIR" ]]; then
        exit 1
fi

chown $UID:$GID /opt/hoover
chown -R $UID:$GID $DATA_DIR
chown $UID:$GID $DATA_DIR/*

exec gosu $USER_NAME "$@"
