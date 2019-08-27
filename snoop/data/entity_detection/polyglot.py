from polyglot.text import Text


NAME = 'polyglot'


def detect(txt, language=None):
    from . import entity_types

    entity_types_map = {
        'I-LOC': entity_types.location,
        'I-ORG': entity_types.organization,
        'I-PER': entity_types.person,
    }

    entities = []
    text = Text(txt, hint_language_code=language)
    for tag_entities in text.entities:
        for entity in tag_entities:
            entities.append({
                'type': entity_types_map[tag_entities.tag],
                'name': entity,
            })
    return entities
