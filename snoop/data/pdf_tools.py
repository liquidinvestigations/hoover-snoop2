"""Helpers for working with PDF files
(splitting into pages, fetching info, extracting text for UI Find tool)"""

import subprocess
import math
import tempfile
import contextlib
import json
from threading import Thread
from subprocess import Popen, PIPE
import logging
import os

log = logging.getLogger(__name__)
MAX_PDF_PAGES_PER_CHUNK = 6000


def write_content_to_handle(content, handle):
    """Write streaming content to file handle, then close it.
    Useful to run in parallel thread."""
    log.warning('writing content to handle %s', handle)
    try:
        for chunk in content:
            if handle.closed:
                break
            handle.write(chunk)
    except Exception as e:
        log.exception(e)
        return
    finally:
        handle.close()


@contextlib.contextmanager
def run_script(script, content, timeout='120s', kill='130s'):
    """Yield a `subprocess.Popen` object that will have the content streamed to stdin."""
    # vandalize script so we drop very long STDERR messages from the logs
    # qpdf is sometimes very spammy with content warnings
    script = script + ' 2> >(head -c200 >&2)'
    cmd = ['/usr/bin/timeout', '-k', kill, timeout, '/bin/bash', '-exo', 'pipefail', '-c', script]
    with tempfile.TemporaryDirectory() as tmpdirname:
        proc = Popen(cmd, stdin=PIPE, stdout=PIPE, cwd=tmpdirname)
        writer = Thread(target=write_content_to_handle, args=(content, proc.stdin))
        writer.start()
        try:
            yield proc
        except Exception as e:
            log.exception(e)
        finally:
            proc.stdin.close()
            proc.stdout.close()
            if proc.poll() is None:
                proc.terminate()
            writer.join()
            if proc.poll() is None:
                proc.kill()
            if proc.returncode != 0 and proc.returncode != 3:
                raise RuntimeError('script failed')


# def stream_script(script, content):
#     with run_script(script, content) as proc

def stream_script(script, content, chunk_size=16 * 1024):
    """Stream content into stdin of script, and generate the stdout."""
    with run_script(script, content) as proc:
        while chunk := proc.stdout.read(chunk_size):
            yield chunk


def get_pdf_info(path):
    """streaming wrapper to extract pdf info json (page count, chunks)"""
#    script = "export JAVA_TOOL_OPTIONS='-Xmx3g'; pdftk - dump_data | grep NumberOfPages | head -n1"
    # script = "pdfinfo -  | grep Pages | head -n1"
    script = f"qpdf --show-npages {path}"
    page_count = int(subprocess.check_output(script, shell=True).decode('ascii'))
    size_mb = round(os.stat(path).st_size / 2**20, 3)
    DESIRED_CHUNK_MB = 30
    chunk_count = max(1, int(math.ceil(size_mb / DESIRED_CHUNK_MB)))
    pages_per_chunk = int(math.ceil((page_count + 1) / chunk_count))
    pages_per_chunk = min(pages_per_chunk, MAX_PDF_PAGES_PER_CHUNK)
    expected_chunk_size_mb = round(size_mb / chunk_count, 3)
    chunks = []
    for i in range(0, chunk_count):
        a = 1 + i * pages_per_chunk
        b = a + pages_per_chunk - 1
        b = min(b, page_count)
        chunks.append(f'{a}-{b}')

    data = {
        'size_mb': size_mb,
        'expected_chunk_size_mb': expected_chunk_size_mb,
        'page_count': page_count,
        'chunks': chunks,
    }
    yield json.dumps(data).encode('ascii', errors='replace')


def split_pdf_file(path, _range):
    """streaming wrapper to split pdf file into a page range."""
    script = (
        " qpdf --empty --no-warn --warning-exit-0 --deterministic-id "
        " --object-streams=generate  --remove-unreferenced-resources=yes "
        " --no-original-object-ids "
        f" --pages {path} {_range}  -- /dev/stdout"
    )
    yield from stream_script(script, [])


def pdf_extract_text(streaming_content):
    """Extract pdf text using script, into a stream."""
    # put streaming content in file, then run script on that file
    with tempfile.NamedTemporaryFile(delete=True, prefix='pdf-extract-in') as infile, \
            tempfile.NamedTemporaryFile(delete=True, prefix='pdf-extract-out') as outfile:
        for chunk in streaming_content:
            infile.write(chunk)
        infile.seek(0)
        script = f'/opt/hoover/snoop/pdf-tools/run.sh file://{infile.name} {outfile.name}'
        for error_msg in stream_script(script, streaming_content):
            log.warning('pdf extract text warning: %s', error_msg)

        outfile.seek(0)
        while chunk := outfile.read(128 * 1024):
            yield chunk
