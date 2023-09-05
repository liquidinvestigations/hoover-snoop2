#!/bin/bash
set -ex
ls -alh $1
# try 3 times before saving error
(
   ( nodejs /opt/hoover/snoop/pdf-tools/extract-text.js $1 $2  \
         || ( echo "KILLED 1st try" && sleep 2 && nodejs /opt/hoover/snoop/pdf-tools/extract-text.js $1 $2 ) \
         || ( echo "KILLED 2nd try" && sleep 3 && nodejs /opt/hoover/snoop/pdf-tools/extract-text.js $1 $2 )
   ) && echo 'extract OK' || echo "PDFJS EXTRACT FAILED: $1"
) 2>&1 | tee "$3" | cat >>/dev/stderr
