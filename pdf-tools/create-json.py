import json
import glob

data = []
for filename in glob.glob('output-*.pdf.json'):
    i = int(filename.split('-')[1].split('.')[0])
    with open(filename, 'r') as f:
        txt = json.load(f)[0]['text']
    # remove \n to make this byte-compatible with the pdfjs implementation
    txt = txt.replace("\n", " ")

    line = {'pageNum': i, 'text': txt}
    data.append(line)
data = sorted(data, key=lambda line: line['pageNum'])
print(json.dumps(data, indent=2))
