def run_once(func):
    not_run = object()
    rv = not_run

    def wrapper():
        nonlocal rv
        if rv is not_run:
            rv = func()
        return rv

    return wrapper
