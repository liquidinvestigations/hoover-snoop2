"""Constant definitions for mime type - file type mapping.

The mime type is the one returned by `libmagic` in `snoop.data.magic`.

The "file type" is a user-friendly category of a mime type. It's stored on the documents as `filetype`, used
for logic in switches, and presented in the UI as a first-class attribute of the document. Examples of file
types: "folder", "email", "archive".

Not all mime types have a file type bound to them.
"""

FILE_TYPES = {
    'application/x-directory': 'folder',
    'application/pdf': 'pdf',
    'text/plain': 'text',
    'text/html': 'html',
    'application/x-hush-pgp-encrypted-html-body': 'html',
    'application/xhtml+xml': 'html',
    'message/x-emlx': 'email',
    'message/rfc822': 'email',
    'application/vnd.ms-outlook': 'email',

    'application/x-hoover-pst': 'email-archive',
    'application/mbox': 'email-archive',

    'application/msword': 'doc',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'doc',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.template': 'doc',
    'application/vnd.ms-word.document.macroEnabled.12': 'doc',
    'application/vnd.ms-word.template.macroEnabled.12': 'doc',
    'application/vnd.oasis.opendocument.text': 'doc',
    'application/vnd.oasis.opendocument.text-template': 'doc',
    'application/rtf': 'doc',

    'application/vnd.ms-excel': 'xls',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xls',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.template': 'xls',
    'application/vnd.ms-excel.sheet.macroEnabled.12': 'xls',
    'application/vnd.ms-excel.template.macroEnabled.12': 'xls',
    'application/vnd.ms-excel.addin.macroEnabled.12': 'xls',
    'application/vnd.ms-excel.sheet.binary.macroEnabled.12': 'xls',
    'application/vnd.oasis.opendocument.spreadsheet-template': 'xls',
    'application/vnd.oasis.opendocument.spreadsheet': 'xls',

    'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'ppt',
    'application/vnd.openxmlformats-officedocument.presentationml.template': 'ppt',
    'application/vnd.openxmlformats-officedocument.presentationml.slideshow': 'ppt',
    'application/vnd.ms-powerpoint': 'ppt',
    'application/vnd.ms-powerpoint.addin.macroEnabled.12': 'ppt',
    'application/vnd.ms-powerpoint.presentation.macroEnabled.12': 'ppt',
    'application/vnd.ms-powerpoint.template.macroEnabled.12': 'ppt',
    'application/vnd.ms-powerpoint.slideshow.macroEnabled.12': 'ppt',
    'application/vnd.oasis.opendocument.presentation': 'ppt',
    'application/vnd.oasis.opendocument.presentation-template': 'ppt',

    'application/zip': 'archive',
    'application/rar': 'archive',
    'application/x-7z-compressed': 'archive',
    'application/x-tar': 'archive',
    'application/x-bzip2': 'archive',
    'application/x-zip': 'archive',
    'application/x-gzip': 'archive',
    'application/x-zip-compressed': 'archive',
    'application/x-rar-compressed': 'archive',
}
"""Mapping from mime types to Hoover file types.


Used by [snoop.data.digests.get_filetype][].
"""
