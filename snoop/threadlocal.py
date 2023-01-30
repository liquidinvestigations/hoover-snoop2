"""Define a `threading.local` global to be used in multiple places.

Required because we can't import any Django-related packages before Django sets itself up,
so, for example, Tracing needs to work (and make use of threadlocal context) without
importing Django.
"""
import threading

threadlocal = threading.local()
