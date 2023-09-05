import os
import json
import glob
import logging
import sys

log = logging.getLogger(__name__)

data = []
os.chdir(sys.argv[1])

for pdf_filename in glob.glob('output-*.pdf'):
    # output-3-15.pdf
    i, j = map(int, pdf_filename.split('.')[0].split('-')[1:3])
    json_filename = pdf_filename + '.json'
    err_filename = pdf_filename + '.stderr'
    assert os.path.exists(err_filename)
    # we don't know if the JSON is valid, or if the file is missing
    chunk = None
    try:
        with open(json_filename, 'r') as f:
            chunk = json.load(f)
    except Exception as e:
        log.warning('failed to parse json %s: %s', json_filename, str(e))
        with open(err_filename, 'r') as f:
            err_txt = f.read()
            line = {'status': 'error', 'error': err_txt, 'err_page_begin': i, 'err_page_end': j}
            data.append(line)
            continue

    for k, item in enumerate(chunk):
        line = {'pageNum': i + k, 'text': item['text'], 'status': 'ok'}
        data.append(line)

data = sorted(data, key=lambda line: line.get('pageNum', -1))
print(json.dumps(data, indent=2))
