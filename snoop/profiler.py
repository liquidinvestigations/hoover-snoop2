import cProfile
from functools import wraps
from pathlib import Path

from django.conf import settings


class Profiler(cProfile.Profile):
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

    def decorator(func):

        @wraps(func)
        def profile_function(*args, **kwargs):
            with Profiler() as profiler:
                profiler.filename = filename
                return func(*args, **kwargs)

        return profile_function

    return decorator
