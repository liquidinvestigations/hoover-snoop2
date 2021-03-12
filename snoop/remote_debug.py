"""Settings and breakpoint for remote debugger.

TODO:
    This is not actively used in the system and should be removed.
"""

from django.conf import settings

REMOTE_DEBUGGER_DEFAULT_PORT = 5678


def remote_breakpoint():
    if getattr(settings, 'REMOTE_DEBUG_ENABLED', False):
        host = getattr(settings, 'REMOTE_DEBUG_HOST', 'localhost')
        port = getattr(settings, 'REMOTE_DEBUG_PORT', REMOTE_DEBUGGER_DEFAULT_PORT)

        import pydevd
        pydevd.settrace(host, port=port, stdoutToServer=True, stderrToServer=True,
                        overwrite_prev_trace=False)
