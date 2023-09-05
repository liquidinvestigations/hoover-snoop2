qpdf --split-pages $1 output-%d.pdf
for i in output-*.pdf; do
        nodejs /opt/hoover/snoop/pdf-tools/extract-text.js $i $i.json
done
rm output-*.pdf

python /opt/hoover/snoop/pdf-tools/create-json.py > $2
