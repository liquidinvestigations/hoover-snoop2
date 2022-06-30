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
from ..collections import current as current_collection

log = logging.getLogger(__name__)

ENTITIES_SUPPORTED_LANGUAGE_CODES = [
    "ar", "bg", "ca", "cs", "da", "de", "el", "en", "es", "et", "fa", "fi", "fr", "he", "hi", "hr", "hu",
    "id", "it", "ja", "ko", "lt", "lv", "ms", "nb", "nl", "no", "pl", "pt", "ro", "ru", "sk", "sl", "sr",
    "sv", "th", "tl", "tr", "uk", "vi", "zh",
]
"""Supported 2 letter language codes for entity extraction."""

LANGUAGE_CODE_MAP = {
    "ar": "ara",
    "bg": "bul",
    "ca": "cat",
    "cs": "ces",
    "da": "dan",
    "de": "deu",
    "el": "ell",
    "en": "eng",
    "es": "spa",
    "et": "est",
    "fa": "fas",
    "fi": "fin",
    "fr": "fra",
    "he": "heb",
    "hi": "hin",
    "hr": "hrv",
    "hu": "hun",
    "id": "ind",
    "it": "ita",
    "ja": "jpn",
    "ko": "kor",
    "lt": "lit",
    "lv": "lav",
    "ms": "msa",
    "nb": "nob",
    "nl": "nld",
    "no": "nor",
    "pl": "pol",
    "pt": "por",
    "ro": "ron",
    "ru": "rus",
    "sk": "slk",
    "sl": "slv",
    "sr": "srp",
    "sv": "swe",
    "th": "tha",
    "tl": "tgl",
    "tr": "tur",
    "uk": "ukr",
    "vi": "vie",
    # there is 2 different chinese that tesseract knows
    # chi_sim and chi_tra
    # also polyglot might return zh_hant
    # it needs to be tested how this behaves with chinese
    # documents
    "zh": "chi_tra",
}
"""Maps ISO 639-1 Codes from Polyglot to ISO 639-2 Codes
from Tesseract.
https://tesseract-ocr.github.io/tessdoc/Data-Files-in-different-versions.html
"""

MAX_ENTITY_TEXT_LENGTH = 200
"""Truncate entities after this length."""

MAX_ENTITY_COUNT_PER_DOC = 10000
"""Stop looking at entitites in a document after this many distinct results."""

ENTITIES_TIMEOUT_BASE = 120
"""Minimum number of seconds to wait for this service."""

ENTITIES_TIMEOUT_MAX = 1200
"""Maximum number of seconds to wait for this service.
For the entities, this comes out at about 5MB of text / request at the min speed.

This should be double the timeout of the service itself, to account for a queue in front of it."""

ENTITIES_MIN_SPEED_BPS = 256
"""Minimum reference speed for this task. Saved as 10% of the Average Success
Speed in the Admin UI. The timeout is calculated using this value, the request
file size, and the previous `TIMEOUT_BASE` constant."""

MAX_LANGDETECT_DOC_READ = 1 * (2 ** 20)  # 1 MB
"""Max text length to read when running language detection."""

TRANSLATION_MIN_SPEED_BPS = 60
"""Minimum reference speed used for translation tasks."""

TRANSLATION_TIMEOUT_BASE = 300
"""Minimum number of seconds to wait for translation service."""

TRANSLATION_TIMEOUT_MAX = 1200
"""Maximum number of seconds to wait for translation service."""

TRANSLATION_SUPPORTED_LANGUAGES = [
    {"code": "en", "name": "English"},
    {"code": "ar", "name": "Arabic"},
    {"code": "az", "name": "Azerbaijani"},
    {"code": "zh", "name": "Chinese"},
    {"code": "cs", "name": "Czech"},
    {"code": "nl", "name": "Dutch"},
    {"code": "fi", "name": "Finnish"},
    {"code": "fr", "name": "French"},
    {"code": "de", "name": "German"},
    {"code": "hi", "name": "Hindi"},
    {"code": "hu", "name": "Hungarian"},
    {"code": "id", "name": "Indonesian"},
    {"code": "ga", "name": "Irish"},
    {"code": "it", "name": "Italian"},
    {"code": "ja", "name": "Japanese"},
    {"code": "ko", "name": "Korean"},
    {"code": "pl", "name": "Polish"},
    {"code": "pt", "name": "Portuguese"},
    {"code": "ru", "name": "Russian"},
    {"code": "es", "name": "Spanish"},
    {"code": "sv", "name": "Swedish"},
    {"code": "tr", "name": "Turkish"},
    {"code": "uk", "name": "Ukranian"},
    {"code": "vi", "name": "Vietnamese"},
]
"""Languages that can be translated to one another, including code and name. Taken copy/paste from the
LibreTranslate API /languages route."""

TRANSLATION_SUPPORTED_LANGUAGE_CODES = [
    a['code']
    for a in TRANSLATION_SUPPORTED_LANGUAGES
]


def can_translate(lang):
    """Checks if we can translate this language."""
    return lang in TRANSLATION_SUPPORTED_LANGUAGE_CODES


def can_extract_entities(lang):
    """Checks if we can extract entities from this language."""
    return lang in ENTITIES_SUPPORTED_LANGUAGE_CODES


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


def call_translate_server(endpoint, data_dict, timeout=TRANSLATION_TIMEOUT_MAX):
    """Calls theh translation server with given arguments."""

    url = f'{settings.TRANSLATION_URL}/{endpoint}'
    resp = requests.post(url, json=data_dict, timeout=timeout)

    if resp.status_code != 200 or resp.headers['Content-Type'] != 'application/json':
        raise SnoopTaskBroken(resp.text, 'translate_http_' + str(resp.status_code))
    return resp.json()


@snoop_task('entities.detect_language', version=1, queue='entities')
@returns_json_blob
def detect_language(blob):
    """Task that runs language detection"""

    if not collections.current().nlp_language_detection_enabled:
        raise SnoopTaskBroken('not enabled', 'nlp_lang_detection_not_enabled')

    digest = models.Digest.objects.get(blob=blob)
    digest_data = digest.result.read_json()
    texts = [digest_data.get('text', "")] + list(digest_data.get('ocrtext', {}).values())
    texts = [t[:MAX_LANGDETECT_DOC_READ].strip() for t in texts if len(t.strip()) > 1]
    texts = "\n\n".join(texts).strip()[:MAX_LANGDETECT_DOC_READ]

    timeout = min(TRANSLATION_TIMEOUT_MAX,
                  int(TRANSLATION_TIMEOUT_BASE + len(texts) / TRANSLATION_MIN_SPEED_BPS))

    lang = call_nlp_server('language_detection', {'text': texts}, timeout)['language']
    return {'lang': lang}


@snoop_task('entities.translate', version=1, queue='translate')
@returns_json_blob
def translate(blob, lang):
    """Task that runs language detection and machine translation.

    Receives text from one document, concatenated from all sources (text and OCR).

    Returns a JSON Blob with detected language, as well as any translations done."""
    if not collections.current().translation_enabled:
        raise SnoopTaskBroken('translation not enabled', 'nlp_translate_not_enabled')

    _txt_limit = collections.current().translation_text_length_limit
    digest = models.Digest.objects.get(blob=blob)
    digest_data = digest.result.read_json()
    tesseract_lang_code = LANGUAGE_CODE_MAP.get(lang)
    if not tesseract_lang_code:
        log.warning(f'No OCR Language code found for language: {lang}')
        # keep all texts if there is no matching language code
        tesseract_lang_code = ""
    if tesseract_lang_code in current_collection().ocr_languages:
        texts = [digest_data.get('text', "")] +\
            [text[1] for text in list(digest_data.get('ocrtext', {}).items())
             if text[0] == f'tesseract_{tesseract_lang_code}']
    else:
        # keep first ocr if no ocr language matches detected language
        texts = [digest_data.get('text', "")] +\
            [[text[1] for text in list(digest_data.get('ocrtext', {}).items())][0]]
    texts = [t[:_txt_limit].strip() for t in texts if len(t.strip()) > 1]
    texts = "\n\n".join(texts).strip()[:MAX_LANGDETECT_DOC_READ]

    timeout = min(TRANSLATION_TIMEOUT_MAX,
                  int(TRANSLATION_TIMEOUT_BASE + len(texts) / TRANSLATION_MIN_SPEED_BPS))

    rv = {'lang': lang}

    texts = texts[:_txt_limit]
    rv['translated-text'] = {}
    rv['translated-from'] = []
    rv['translated-to'] = []
    for target in settings.TRANSLATION_TARGET_LANGUAGES:
        if target == lang:
            log.warning("skipping translation from %s into %s", lang, target)
            continue
        tr_source = lang or 'auto'
        log.info('translating length=%s from %s to %s', len(texts), tr_source, target)
        tr_args = {'q': texts, 'source': tr_source, 'target': target}
        tr_text = call_translate_server('translate', tr_args, timeout)
        tr_text = tr_text.get('translatedText', '').strip()
        if tr_text and len(tr_text) > 1:
            rv['translated-text'][f'translated_{tr_source}_to_{target}'] = tr_text
            rv['translated-from'].append(tr_source)
            rv['translated-to'].append(target)
        else:
            log.warning("failed translation from %s into %s", lang, target)

    return rv


@snoop_task('entities.get_entity_results', version=3, queue='entities')
@returns_json_blob
def get_entity_results(blob, language=None, translation_result_pk=None):
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
        translation_result_pk: blob containing JSON with extra text sources to process.

    Returns:
        The responses from the calls to the NLP Server for all text sources.
    """
    if not current_collection().nlp_entity_extraction_enabled \
            or not can_extract_entities(language):
        raise SnoopTaskBroken('entity extraction disabled', 'nlp_entity_extraction_disabled')

    digest = models.Digest.objects.get(blob=blob)
    timeout = min(ENTITIES_TIMEOUT_MAX,
                  int(ENTITIES_TIMEOUT_BASE + blob.size / ENTITIES_MIN_SPEED_BPS))
    text_limit = collections.current().nlp_text_length_limit

    digest_data = digest.result.read_json()

    text_sources = {}

    if digest_data.get('text'):
        text_sources['text'] = digest_data.get('text', '')[:text_limit]
    tesseract_lang_code = LANGUAGE_CODE_MAP.get(language)
    if not tesseract_lang_code:
        log.warning(f'No OCR Language code found for language: {language}')
        # keep all texts if there is no matching language code
        tesseract_lang_code = ""
    if digest_data.get('ocrtext') and tesseract_lang_code in current_collection().ocr_languages:
        for k, v in digest_data.get('ocrtext').items():
            if not k == f'tesseract_{tesseract_lang_code}':
                continue
            if v:
                text_sources[k] = v[:text_limit]
    elif digest_data.get('ocrtext'):
        # no ocr language matches the detecte language so we only
        # keep the first ocr text
        first_ocr = list(digest_data.get('ocrtext').items())[0]
        text_sources[first_ocr[0]] = first_ocr[1][:text_limit]

    if translation_result_pk:
        log.info('loaded language data')
        lang_result_json = models.Blob.objects.get(pk=translation_result_pk).read_json()
        if lang_result_json.get('translated-text'):
            for k, v in lang_result_json.get('translated-text').items():
                if v:
                    text_sources[k] = v[:text_limit]

    collected_responses = []
    for source, text in text_sources.items():
        response = {'entities': None, 'language': None, 'source': source}
        data = {'text': text}
        if settings.EXTRACT_ENTITIES:
            if language:
                data['language'] = language
                response['language'] = language
            response['entities'] = call_nlp_server('entity_extraction', data, timeout)

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
        # rv['lang'].append(entity_result['entities']['language'])
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
    # if rv['lang']:
    #     # rv['lang'] = list(set(rv['lang']))
    #     rv['lang'] = rv['lang'][0]
    return rv
