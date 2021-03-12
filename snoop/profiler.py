"""Definition for profile() decorator using cProfile.

This uses some undefined and unused django settings (PROFILING_ENABLED, DEFAUILT_PROFILING_FILE) to decide
when to run and where to dump its output. It seems that it will overwrite the DEFAULT_PROFILING_FILE output
every time a function decorated with `profile()` will finish running, so it won't keep a history beyond the
last invocation.

TODO:
    This system hasn't been used in a while,  but probably still works. We should see if it's of any value,
    or if this functionality should be recreated outside of snoop, with `python -m profile ./manage.py
    retrytask --fg`.
"""

import cProfile
from functools import wraps
from pathlib import Path

from django.conf import settings


class Profiler(cProfile.Profile):
    """Context manager that dumps cProfile stats on __exit__."""
    filename = None

    def __enter__(self):
        if getattr(settings, 'PROFILING_ENABLED', False):
            self.enable(builtins=False)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if not getattr(settings, 'PROFILING_ENABLED', False):
            return

        self.disable()
        self.create_stats()
        if not self.filename and not getattr(settings, 'DEFAULT_PROFILING_FILE', None):
            self.print_stats()
        else:
            filename = self.filename or getattr(settings, 'DEFAULT_PROFILING_FILE', None)
            profile_file = Path(__file__).absolute().parent.parent / \
                settings.PROFILES_DIR / filename
            self.dump_stats(str(profile_file))


def profile(filename=None):
    """Decorator that dumps cProfile stats to file after function finished."""

    def decorator(func):

        @wraps(func)
        def profile_function(*args, **kwargs):
            with Profiler() as profiler:
                profiler.filename = filename
                return func(*args, **kwargs)

        return profile_function

    return decorator
