import os
import logging

log = logging.getLogger(__name__)


# gunicorn configuration to get stable worker id
# https://gist.github.com/hynek/ba655c8756924a5febc5285c712a7946
def on_starting(server):
    """
    Attach a set of IDs that can be temporarily re-used.
    Used on reloads when each worker exists twice.
    """
    server._worker_id_overload = set()


def nworkers_changed(server, new_value, old_value):
    """
    Gets called on startup too.
    Set the current number of workers.  Required if we raise the worker count
    temporarily using TTIN because server.cfg.workers won't be updated and if
    one of those workers dies, we wouldn't know the ids go that far.
    """
    server._worker_id_current_workers = new_value


def _next_worker_id(server):
    """
    If there are IDs open for re-use, take one.  Else look for a free one.
    """
    if server._worker_id_overload:
        return server._worker_id_overload.pop()

    in_use = set(w._worker_id for w in tuple(server.WORKERS.values()) if w.alive)
    free = set(range(1, server._worker_id_current_workers + 1)) - in_use

    return free.pop()


def on_reload(server):
    """
    Add a full set of ids into overload so it can be re-used once.
    """
    server._worker_id_overload = set(range(1, server.cfg.workers + 1))


def pre_fork(server, worker):
    """
    Attach the next free worker_id before forking off.
    """
    worker._worker_id = _next_worker_id(server)


def post_fork(server, worker):
    from snoop.data.s3 import clear_mounts, refresh_worker_index
    from snoop.data.tracing import init_tracing

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "snoop.defaultsettings")
    server.log.debug("Gunicorn Worker spawned #%s (pid: %s)", worker._worker_id, worker.pid)

    os.environ["GUNICORN_WORKER_ID"] = str(worker._worker_id)

    init_tracing('gunicorn')

    try:
        refresh_worker_index()
        clear_mounts()
    except Exception as e:
        server.log.exception(e)


# Signal handler to flush sentry before shutting off process.
def worker_exit(server, worker):
    if os.getenv('SENTRY_DSN'):
        try:
            log.debug('gworker: flushing sentry...')
            from sentry_sdk import Hub
            client = Hub.current.client
            if client:
                client.flush()
        except Exception as e:
            log.warning('gworker: could not flush sentry: %s', str(e))
