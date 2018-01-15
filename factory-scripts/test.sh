#!/bin/bash -ex

cd "$(dirname "$0")"

if [ ! -d factory ]; then
    ./install.sh
fi

echo "Running tests"
factory/factory run --share ..:/opt/snoop2 /opt/snoop2/factory-scripts/guest/test.sh
