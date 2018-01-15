#!/bin/bash -ex

cd "$(dirname "$0")"

if [ ! -d factory ]; then
    ./install.sh
fi

echo "Running interactive shell"
factory/factory login --share ..:/opt/snoop2
