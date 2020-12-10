#!/bin/bash -ex

[ -d "/opt/hoover/snoop/blobs" ] && chown $UID:$GID -R /opt/hoover/snoop/blobs

chown $UID:$GID $DATA_DIR # snoop celery

exec gosu $USER_NAME "$@"