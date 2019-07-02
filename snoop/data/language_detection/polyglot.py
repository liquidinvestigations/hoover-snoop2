from polyglot.detect import Detector


NAME = 'polyglot'


def detect(txt):
    detector = Detector(txt)
    language = detector.language
    if language.code and language.code != 'un' and detector.reliable:
        return detector.language.code
    return None
