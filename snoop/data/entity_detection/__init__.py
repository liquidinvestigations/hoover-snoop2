from collections import namedtuple

from . import polyglot


EntityTypes = namedtuple('EntityTypes', ['location', 'organization', 'person'])
entity_types = EntityTypes('location', 'organization', 'person')


detectors = {}


def register_detector(name, detector):
    detectors[name] = detector


register_detector(polyglot.NAME, polyglot.detect)
