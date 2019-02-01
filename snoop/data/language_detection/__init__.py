from django.conf import settings

from . import langdetect, polyglot


detectors = {}


def register_detector(name, detector):
    detectors[name] = detector


register_detector(langdetect.NAME, langdetect.detect)
register_detector(polyglot.NAME, polyglot.detect)
