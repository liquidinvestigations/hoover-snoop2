import os

from snoop.data.tracing import init_tracing


def post_fork(server, worker):
    server.log.info("Worker spawned (pid: %s)", worker.pid)

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "snoop.defaultsettings")

    init_tracing('gunicorn')
