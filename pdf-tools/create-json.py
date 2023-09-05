import json
import glob

data = []
for filename in glob.glob('output-*.pdf.json'):
    # output-3-15.pdf.json
    i, j = map(int, filename.split('.')[0].split('-')[1:3])
    with open(filename, 'r') as f:
        chunk = json.load(f)

    for k, item in enumerate(chunk):
        line = {'pageNum': i + k, 'text': item['text']}
        data.append(line)
data = sorted(data, key=lambda line: line['pageNum'])
print(json.dumps(data, indent=2))
