#!/bin/bash -ex

echo "Running in $PWD"

mkdir -p /opt/hoover/snoop/testsuite/volumes/snoop-pg/data
chown $UID:$GID -R /opt/hoover/snoop/testsuite/volumes/snoop-pg/data

chown -R 666:666 /runserver
chown -R 666:666 /opt/magic-definitions
chown -R 666:666 /opt/libpst

if [ -f ./celerybeat-schedule ]; then
  echo "./celerybeat-schedule found"
  chown 666:666 ./celerybeat-schedule
fi
if [ -f celerybeat-schedule ]; then
  echo "celerybeat-schedule found"
  chown 666:666 celerybeat-schedule
fi

if -f celerybeat-schedule; then
  echo "celerybeat-schedule found"
  chown 666:666 celerybeat-schedule
fi

if -f "celerybeat-schedule"; then
  echo "celerybeat-schedule found mit ho"
  chown 666:666 celerybeat-schedule
fi



chown $UID:$GID $DATA_DIR/*

whoami

echo $SNOOP_DB

exec gosu $USER_NAME "$@"
#sudo -Eu $USER_NAME "$@"
#sudo -Eu $USER_NAME /wait && /runserver
