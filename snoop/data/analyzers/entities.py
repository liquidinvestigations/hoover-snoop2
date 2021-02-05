"""Tasks for entity extraction and language detection.

"""

import logging
from collections import defaultdict

import requests
from django.conf import settings
from django.db import transaction
from django.db import connections

from .. import models
from .. import collections
from ..tasks import snoop_task, SnoopTaskBroken, returns_json_blob

log = logging.getLogger(__name__)

MAX_ENTITY_TEXT_LENGTH = 200
"""Truncate entities after this length."""

MAX_ENTITY_COUNT_PER_DOC = 10000
"""Stop looking at entitites in a document after this many distinct results."""

MAX_ENTITY_DOC_READ = 5 * (2 ** 20)  # 5 MB, about one King James Bible
"""Stop looking at the document text after this many characters."""

ENTITIES_TIMEOUT_BASE = 120
"""Minimum number of seconds to wait for this service."""

ENTITIES_TIMEOUT_MAX = 3600
"""Maximum number of seconds to wait for this service.
For the entities, this comes out at about 5MB of text / request at the min speed."""

ENTITIES_MIN_SPEED_BPS = 512
"""Minimum reference speed for this task. Saved as 10% of the Average Success
Speed in the Admin UI. The timeout is calculated using this value, the request
file size, and the previous `TIMEOUT_BASE` constant."""


def call_nlp_server(endpoint, data_dict, timeout=ENTITIES_TIMEOUT_MAX):
    """Calls the nlp server with data at an endpoint

    Sends a request to the nlp service with a specified endpoint and a data load as json.
    The two endpoints are `language_detection` and `entity extraction` at the moment.
    The `data_dict` is a dictionary which contains the request. It needs to contain a key `text`
    and can optionally also contain a key `language`, if the language is already specified.
    The response of the service will be JSON, which is decoded before returning.

    Args:
        endpoint: The name of the server endpoint that's called.
        data_dict: A dictionary containing data for a post request.

    Returns:
        The parsed JSON-response of the server

    Raises:
        ConnectionError: If the server was not found
        RuntimeError: If the server returns with an unexpected result.
        NotImplementedError: If the server is not able to process the request.
    """
    url = f'{settings.SNOOP_NLP_URL}/{endpoint}'
    resp = requests.post(url, json=data_dict, timeout=timeout)

    if resp.status_code != 200 or resp.headers['Content-Type'] != 'application/json':
        raise SnoopTaskBroken(resp.text, 'nlp_http_' + str(resp.status_code))
    return resp.json()


@snoop_task('entities.get_entity_results')
@returns_json_blob
def get_entity_results(blob, language=None):
    """ Gets all entities and the language for all text sources in a digest.

    Creates a dict from a string and requests entity extraction from the nlp service
    optionally, the language can be specified.
    returns the dict with the collected responses from the service.

    Args:
        digest_id: digest ID for the document to process. The different types of text sources will be
        extracted from the digest and send to the nlp service to perform entity recognition.
        language: an optional language code, telling the service which language
        to use in order to process the text, instead of figuring that out by
        itself.

    Returns:
        The responses from the calls to the NLP Server for all text sources.
    """
    digest = models.Digest.objects.get(blob=blob)
    timeout = min(ENTITIES_TIMEOUT_MAX,
                  int(ENTITIES_TIMEOUT_BASE + blob.size / ENTITIES_MIN_SPEED_BPS))

    digest_data = digest.result.read_json()

    text_sources = {}

    if digest_data.get('text'):
        text_sources['text'] = digest_data.get('text', '')[:MAX_ENTITY_DOC_READ]

    if digest_data.get('ocrtext'):
        for ocr_name, ocr_text in digest_data.get('ocrtext').items():
            if ocr_text:
                text_sources['ocrtext_' + ocr_name] = ocr_text[:MAX_ENTITY_DOC_READ]

    collected_responses = []
    for source, text in text_sources.items():
        response = {'entities': None, 'language': None, 'source': source}
        data = {'text': text}
        if settings.EXTRACT_ENTITIES:
            if language:
                data['language'] = language
            response['entities'] = call_nlp_server('entity_extraction', data, timeout)

        if settings.DETECT_LANGUAGE:
            response['language'] = call_nlp_server('language_detection', data, timeout)['language']

        collected_responses.append(response)

    return collected_responses


def translate_entity_type(old_type):
    """Translate from Spacy or Polyglot entity types into our types.

    Here are all the types:
        - location
        - organization
        - event
        - person
        - money

    Returns None if old type is not interesting and entity can be discarded.
    """

    LOC = 'location'
    ORG = 'organization'
    EVT = 'event'
    PER = 'person'
    MON = 'money'

    tr = {
        # SPACY
        # =====
        # 'CARDINAL': 'number',  # Numerals that do not fall under another typ
        # 'DATE': 'date',  # Absolute or relative dates or periods
        # 'DATETIME',
        'EVENT': EVT,  # Named hurricanes, battles, wars, sports events, etc.
        'FAC': LOC,
        'FACILITY': LOC,  # Buildings, airports, highways, bridges, etc.
        'GPE': LOC,  # Countries, cities, states
        # 'LANGUAGE': 'language',  # Any named language
        # 'LAW': 'law',  # Named documents made into laws
        'LOC': LOC,
        'LOCATION': LOC,  # Non-GPE locations, mountain ranges, bodies of water
        'MONEY': MON,  # Monetary values, including unit
        'NAT_REL_POL': ORG,
        'NORP': ORG,  # Nationalities or religious or political groups
        # 'NUMERIC_VALUE',
        # 'ORDINAL',
        # 'ORDINAL': LOC,  # “first”, “second”
        'ORG': ORG,
        'ORGANIZATION': ORG,  # Companies, agencies, institutions, etc.
        # 'PERCENT': LOC,  # Percentage (including “%”)
        # 'PERIOD',
        'PER': PER,
        'PERSON': PER,  # People, including fictional
        'PRODUCT': ORG,  # Vehicles, weapons, foods, etc. (Not services)
        # 'QUANTITY',
        # 'QUANTITY': 'number',  # Measurements, as of weight or distance
        # 'TIME': 'date',  # Times smaller than a day
        'WORK': ORG,  # OF ART Titles of books, songs, etc.
        'WORK_OF_ART': ORG,

        # POLYGLOT
        # ========
        'I-LOC': LOC,
        'I-ORG': ORG,
        'I-PER': PER,
    }
    return tr.get(old_type, None)


@transaction.atomic
def create_db_entries(ents):
    """Creates all Database entries which are related to an entity.

    `get_or_create` is used for the entity, so that every entity is only once in
    the database. In the Hit, the information where that entity was found is stored.
    Args:
        ents: A list of dicts with the following keys:
            - entity: dict containing entity text and type as Strings.
            - model: the name of the used model
            - language: language code of the used model
            - text_source: 'text' or the OCR text source
            - digest: The digest object where the entity was found in.

    Returns:
        The Ids of the created entity objects(if any).
    """

    def lock_table(model):
        col = collections.current()
        with connections[col.db_alias].cursor() as cursor:
            query = f'LOCK TABLE {model._meta.db_table}'
            log.info('collection %s: %s', col.name, query)
            cursor.execute(query)

    for ent in ents:
        ent['engine'] = 'polyglot' if ent['model'].startswith('polyglot_') else 'spacy'

    # Translate type from the 20+ Spacy types and the 3 Polyglot types into our 5 types.
    for ent in ents:
        ent['entity']['type'] = translate_entity_type(ent['entity']['type'])
    # Exclude entities with no translated type
    ents = [e for e in ents if e['entity']['type']]

    lock = False
    ent_types = {}
    # get new types to insert
    sorted_ent_types = sorted(set(e['entity']['type'] for e in ents))
    # if we have new types, lock table
    if not all(models.EntityType.objects.filter(type=_type).exists()
               for _type in sorted_ent_types):
        lock = True
        lock_table(models.EntityType)
    # after a potentially blocking table lock, do a new get/create
    for _type in sorted_ent_types:
        ent_types[_type], _ = models.EntityType.objects.get_or_create(type=_type)

    ent_items = {}
    sorted_ent_items = sorted(set([(e['entity']['text'], e['entity']['type']) for e in ents]))
    if lock or not all(models.Entity.objects.filter(type=ent_types[_type], entity=_text).exists()
                       for _text, _type in sorted_ent_items):
        lock = True
        lock_table(models.Entity)
    for _text, _type in sorted_ent_items:
        ent_items[(_text, _type)], _ = models.Entity.objects.get_or_create(
            entity=_text,
            type=ent_types[_type],
        )

    lang_models = {}
    sorted_lang_models = sorted(set([(e['language'], e['engine'], e['model']) for e in ents]))
    if lock or not all(models.LanguageModel.objects
                       .filter(language_code=_lang, engine=_eng, model_name=_mod)
                       .exists()
                       for _lang, _eng, _mod in sorted_lang_models):
        lock = True
        lock_table(models.LanguageModel)
    for _lang, _eng, _mod in sorted_lang_models:
        lang_models[(_lang, _eng, _mod)], _ = models.LanguageModel.objects.get_or_create(
            language_code=_lang,
            engine=_eng,
            model_name=_mod,
        )

    hit_list = []
    for e in ents:
        ent_item = ent_items[(e['entity']['text'], e['entity']['type'])]
        ent_lang_model = lang_models[(e['language'], e['engine'], e['model'])]

        hit, _ = models.EntityHit.objects.get_or_create(
            entity=ent_item,
            model=ent_lang_model,
            text_source=e['text_source'],
            start=e['entity']['start'],
            end=e['entity']['end'],
            digest=e['digest'],
        )
        hit_list.append(hit)

    return [hit.pk for hit in hit_list]


def clean_entity_text(text):
    """Try and sanitize the entity text a little bit.

    Does the following:
        - limit text length
        - replaces newlines with spaces
        - folds multiple spaces
        - strips spaces at start/end
    """

    text = text[:MAX_ENTITY_TEXT_LENGTH]
    text = text.replace('\n', ' ')
    text = text.replace('\t', ' ')
    text = text.replace('\r', ' ')
    text = text.replace('  ', ' ')
    text = text.strip()
    return text


def process_results(digest, entity_result_parts):
    """Processes entity extraction and language detection for one document.

    This functions receives the collected results from the service, the database entries are created
    and the IDs of all entities are received from the database.
    Also, a list of all entity types which occur in the document is created.

    Args:
        digest: The digest object for which entitity extraction is done.
        entity_result: The entity results for a single text source.

    Returns:
        results: A dictionary containing all found entities, the IDs of all
        unique entities that were found, as well as keys for all entity types
        containing a list of all entities with that type.
    """
    for entity_result in entity_result_parts:
        # truncate number of entities returned
        entity_result['entities']['entities'] = \
            entity_result['entities']['entities'][:MAX_ENTITY_COUNT_PER_DOC]

        for e in entity_result['entities']['entities']:
            e['text'] = clean_entity_text(e['text'])

    ent_ids = list(set(
        create_db_entries(
            sum(
                [
                    [
                        {
                            'entity': entity,
                            'model': entity_result['entities']['model'],
                            'language': entity_result['entities']['language'],
                            'text_source': entity_result['source'],
                            'digest': digest,
                        }
                        for entity in entity_result['entities']['entities']
                    ]
                    for entity_result in entity_result_parts
                ],
                start=[]
            )
        )
    ))
    log.info('Saved %s entity hit IDs', len(ent_ids))

    rv = defaultdict(list)
    for entity_result in entity_result_parts:
        rv['lang'].append(entity_result['entities']['language'])
        rv['entity'] += [
            k['text']
            for k in entity_result['entities']['entities']
            if k['type']
        ]

        # clone into sub-fields, ordered by type
        for k in entity_result['entities']['entities']:
            entity_type = k['type']
            if not entity_type:
                continue
            rv[f'entity-type.{entity_type}'].append(k['text'])

    # delete empty keys
    for k in list(rv.keys()):
        if not rv[k]:
            del rv[k]

    # remove DefaultDict wrapper
    rv = dict(rv)

    # fold together the various languages, pick the first
    if rv['lang']:
        # rv['lang'] = list(set(rv['lang']))
        rv['lang'] = rv['lang'][0]
    return rv
