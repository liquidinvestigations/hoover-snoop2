#!/bin/bash
set -ex

# through trial and error, i found out these configs
# will take around 6-7GB RAM and minimize runtime
SPLIT_NPAGES=150
MAX_PARALLEL=6

(
qpdf --split-pages=$SPLIT_NPAGES $1 output.pdf

for i in output*.pdf; do echo $i; done | xargs -P$MAX_PARALLEL -I{} bash /opt/hoover/snoop/pdf-tools/run-one.sh "$PWD/{}" "$PWD/{}.json" "$PWD/{}.stderr"

python /opt/hoover/snoop/pdf-tools/create-json.py $PWD > $2
) >> /dev/stderr
