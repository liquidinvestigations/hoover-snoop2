#!/bin/bash -ex

echo "Running in $PWD"

# mkdir -p /opt/hoover/snoop/testsuite/volumes/snoop-pg/data
# chown $UID:$GID -R /opt/hoover/snoop/testsuite/volumes/snoop-pg/data

chown 666:666 /opt/hoover
chown -R 666:666 /opt/hoover/snoop

# file="celerybeat-schedule"
# if [ -f "$file" ]
# then
# 	echo "$file found."
# else
# 	echo "$file not found."
# fi
#
# file="./celerybeat-schedule"
# if [ -f "$file" ]
# then
# 	echo "$file found."
# else
# 	echo "$file not found."
# fi

ls

chown $UID:$GID $DATA_DIR/*

whoami

echo $SNOOP_DB

exec gosu $USER_NAME "$@"
#sudo -Eu $USER_NAME "$@"
#sudo -Eu $USER_NAME /wait && /runserver
