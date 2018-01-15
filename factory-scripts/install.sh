#!/bin/bash -ex

cd "$(dirname "$0")"

echo "Installing Factory"
python3 <(curl -sL https://github.com/liquidinvestigations/factory/raw/master/install.py) factory --image 'https://jenkins.liquiddemo.org/job/liquidinvestigations/job/factory/job/master/lastSuccessfulBuild/artifact/artful-x86_64.factory.gz'

echo "Installing Dependencies on VM"
factory/factory --commit --yes run --share ..:/opt/snoop2 /opt/snoop2/factory-scripts/guest/setup-artful.sh
