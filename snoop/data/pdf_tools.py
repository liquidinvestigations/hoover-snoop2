import tempfile
import contextlib
import json
from threading import Thread
from subprocess import Popen, PIPE
import logging

log = logging.getLogger(__name__)


def write_content_to_handle(content, handle):
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


# def stream_script(script, content):
#     with run_script(script, content) as proc

def stream_script(script, content, chunk_size=16 * 1024):
    with run_script(script, content) as proc:
        while chunk := proc.stdout.read(chunk_size):
            yield chunk


def get_pdf_info(streaming_content):
    """Middleware streaming wrapper to extract pdf info using PDFTK and return it as json content"""
#    script = "export JAVA_TOOL_OPTIONS='-Xmx3g'; pdftk - dump_data | grep NumberOfPages | head -n1"
    script = "pdfinfo -  | grep Pages | head -n1"

    with run_script(script, streaming_content) as proc:
        for line in proc.stdout.readlines():
            log.warning('line %s', line)
            key, val = tuple(map(str.strip, line.decode('ascii', errors='replace').split(':')))
            yield json.dumps({key: val}).encode('ascii', errors='replace')
            break


def split_pdf_file(streaming_content, _range):
    """Middleware streaming wrapper to split pdf file into a page range using pdftk."""
    page_start, page_end = tuple(map(int, _range.split('-')))
    # script = f"""cat > in.pdf && ( ( pdfseparate -f {page_start} -l {page_end} in.pdf 'out_%09d.pdf' && pdfunite out_*.pdf out.pdf ) 1>&2 )  && cat out.pdf"""  # noqa: E501
    # script = f"export JAVA_TOOL_OPTIONS='-Xmx4g'; pdftk - cat '{_range}' output -"
    script = f"cat > in.pdf && qpdf --empty --pages in.pdf {_range} -- /dev/stdout"
    yield from stream_script(script, streaming_content)


def pdf_extract_text(streaming_content, method):
    """Middleware streaming wrapper to extract pdf text using PDF.js (for parity with frontend)"""
    script = {
        'pdftotext': "pdftotext - /dev/stdout",
        'nodejs': 'nodejs /opt/hoover/search/pdf_tools/extract_text.js',
    }[method or 'nodejs']
    yield from stream_script(script, streaming_content)
