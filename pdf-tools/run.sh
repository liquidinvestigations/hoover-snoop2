#!/bin/bash
set -ex

(
pwd
ls

qpdf --split-pages=400 $1 output.pdf
ls -sh

for i in output*.pdf; do echo $i; done | xargs -P6 -I{} bash /opt/hoover/snoop/pdf-tools/run-one.sh "$PWD/{}" "$PWD/{}.json"
# for i in output*.pdf; do
#         echo nodejs /opt/hoover/snoop/pdf-tools/extract-text.js $i $i.json
#         echo
#         echo
#         nodejs /opt/hoover/snoop/pdf-tools/extract-text.js $i $i.json
# done

rm output*.pdf

ls -sh

python /opt/hoover/snoop/pdf-tools/create-json.py > $2
) >> /dev/stderr
