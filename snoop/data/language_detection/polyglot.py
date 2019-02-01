from polyglot.detect import Detector


NAME='polyglot'

def detect(txt):
    detector = Detector(txt)
    if detector.language.code and detector.language.code != 'un' and \
            detector.reliable:
        return detector.language.code
    return None
