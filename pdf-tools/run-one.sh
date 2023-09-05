#!/bin/bash
set -ex
ls -alh $1
touch $2
ls -alh $2
exec nodejs /opt/hoover/snoop/pdf-tools/extract-text.js $1 $2
