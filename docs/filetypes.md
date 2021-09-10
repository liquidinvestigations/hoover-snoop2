# File Types Analyzed by hoover-snoop2 #
## List of mime types ##
| Mime type                                                                 | Hoover Filetype | About                                    | Analyzers           |
| ---                                                                       | ---             | ---                                      | ---                 |
| application/x-7z-compressed                                               | archive         | 7-Zip                                    | archives            |
| application/zip                                                           | archive         | Zip-Archive                              | archives            |
| application/rar                                                           | archive         | RAR-Archive                              | archives            |
| application/x-rar                                                         | archive         | RAR-Archive                              | archives            |
| application/x-zip                                                         | archive         | Zip-Archive                              | archives            |
| application/x-gzip                                                        | archive         | GZip Compressed Archive                  | archives            |
| application/gzip                                                          | archive         | GZip Compressed Archive                  | archives            |
| application/x-bzip2                                                       | archive         | BZip2-Archive                            | archives            |
| application/x-tar                                                         | archive         | Tape Archive (TAR)                       | archives            |
| application/x-hoover-pst                                                  | email-archive   |                                          | archives            |
| application/mbox                                                          | -               | Mbox database files                      | archives            |
| application/pdf                                                           | pdf             | Adobe Portable Document Format           | archives, tika, ocr |
| text/plain                                                                | text            | Text                                     | tika                |
| text/html                                                                 | html            | HyperText Markup Language (HTML)         | tika, html          |
| text/rtf                                                                  | doc             | Rich Text Format (RTF)                   | tika                |
| application/msword                                                        | doc             | Microsoft Word                           | tika                |
| application/vnd.openxmlformats-officedocument.wordprocessingml.document   | doc             | Microsoft Word                           | tika                |
| application/vnd.openxmlformats-officedocument.wordprocessingml.template   | doc             | Microsoft Word                           | tika                |
| application/vnd.ms-word.document.macroEnabled.12                          | doc             | Microsoft Word                           | tika                |
| application/vnd.ms-word.template.macroEnabled.12                          | doc             | Microsoft Word Document Template         | tika                |
| application/vnd.oasis.opendocument.text                                   | doc             | OpenDocument text document               | tika                |
| application/vnd.oasis.opendocument.text-template                          | doc             | OpenDocument text template               | tika                |
| application/rtf                                                           | doc             | Rich Text Format (RTF)                   | tika                |
| application/vnd.ms-excel                                                  | xls             | Microsoft Excel                          | tika                |
| application/vnd.openxmlformats-officedocument.spreadsheetml.sheet         | xls             | Microsoft Excel                          | tika                |
| application/vnd.openxmlformats-officedocument.spreadsheetml.template      | xls             | Microsoft Excel Template                 | tika                |
| application/vnd.ms-excel.sheet.macroEnabled.12                            | xls             | Microsoft Excel                          | tika                |
| application/vnd.ms-excel.template.macroEnabled.12                         | xls             | Microsoft Excel Template                 | tika                |
| application/vnd.ms-excel.addin.macroEnabled.12                            | xls             | Microsoft Excel                          | tika                |
| application/vnd.ms-excel.sheet.binary.macroEnabled.12                     | xls             | Microsoft Excel                          | tika                |
| application/vnd.oasis.opendocument.spreadsheet-template                   | xls             | OpenDocument spreadsheet template        | tika                |
| application/vnd.oasis.opendocument.spreadsheet                            | xls             | OpenDocument spreadsheet document        | tika                |
| application/vnd.openxmlformats-officedocument.presentationml.presentation | ppt             | Microsoft PowerPoint                     | tika                |
| application/vnd.openxmlformats-officedocument.presentationml.template     | ppt             | Microsoft PowerPoint Template            | tika                |
| application/vnd.openxmlformats-officedocument.presentationml.slideshow    | ppt             | Microsoft PowerPoint                     | tika                |
| application/vnd.ms-powerpoint                                             | ppt             | Microsoft PowerPoint                     | tika                |
| application/vnd.ms-powerpoint.addin.macroEnabled.12                       | ppt             | Microsoft PowerPoint                     | tika                |
| application/vnd.ms-powerpoint.presentation.macroEnabled.12                | ppt             | Microsoft PowerPoint                     | tika                |
| application/vnd.ms-powerpoint.template.macroEnabled.12                    | ppt             | Microsoft PowerPoint Template            | tika                |
| application/vnd.ms-powerpoint.slideshow.macroEnabled.12                   | ptt             | Microsoft PowerPoint                     | tika                |
| application/vnd.oasis.opendocument.presentation                           | ppt             | OpenDocument presentation document       | tika                |
| application/vnd.oasis.opendocument.presentation-template                  | ppt             | OpenDocument presentation template       | tika                |
| application/xhtml+xml                                                     | -               | XHTML                                    | html                |
| application/xml                                                           | -               | XML                                      | html                |
| text/xml                                                                  | -               | XML                                      | htlm                |
| image/tiff                                                                | image           | Tagged Image File Format (TIFF)          | exif, ocr           |
| image/jpeg                                                                | image           | JPEG image                               | exif, ocr           |
| image/webp                                                                | image           | WEBP image                               | exif, ocr           |
| image/heic                                                                | image           | High Efficiency Image File Format (HEIF) | exif, ocr           |
| application/vnd.ms-outlook                                                | email           | Outlook MSG file                         | email               |
| application/vnd.ms-office                                                 | -               |                                          | email               |
| application/CDFV2                                                         | -               | Composite Document File V2               | email               |
| message/rfc822                                                            | email           | RFC822 Message                           | email               |
| message/x-emlx                                                            | email           | Apple Email                              | emlx                |
| image/png                                                                 | image          | Portable Network Graphics (PNG)          | ocr                 |
| image/bmp                                                                 | image          | Bitmap Image File (BMP)                  | ocr                 |
| image/gif                                                                 | image          | Graphics Interchange Format (GIF))       | ocr                 |
| image/x-portable-anymap                                                   | image          | Portable Any Map Image (PNM)             | ocr                 |
| image/jp2                                                                 | image          | JPEG 2000 Core Image (JP2)               | ocr                 |

## Code doing the analysis ##
[archives-analyzer](https://github.com/liquidinvestigations/hoover-snoop2/blob/8a6dbdf1fd1ea56db386628f0559b097cdaa1a61/snoop/data/analyzers/archives.py#L11-L31)  
[Files forwarded to tika](https://github.com/liquidinvestigations/hoover-snoop2/blob/8a6dbdf1fd1ea56db386628f0559b097cdaa1a61/snoop/data/analyzers/tika.py#L10-L44)  
[HTML-analyzer](https://github.com/liquidinvestigations/hoover-snoop2/blob/8a6dbdf1fd1ea56db386628f0559b097cdaa1a61/snoop/data/analyzers/html.py#L10-L13)  
[Images-analyzer](https://github.com/liquidinvestigations/hoover-snoop2/blob/8a6dbdf1fd1ea56db386628f0559b097cdaa1a61/snoop/data/analyzers/exif.py#L9)  
[Outlook-email](https://github.com/liquidinvestigations/hoover-snoop2/blob/8a6dbdf1fd1ea56db386628f0559b097cdaa1a61/snoop/data/analyzers/email.py#L18-L21)  
[Normal email](https://github.com/liquidinvestigations/hoover-snoop2/blob/8a6dbdf1fd1ea56db386628f0559b097cdaa1a61/snoop/data/filesystem.py#L123)  
[Apple email](https://github.com/liquidinvestigations/hoover-snoop2/blob/8a6dbdf1fd1ea56db386628f0559b097cdaa1a61/snoop/data/filesystem.py#L112)  

[Grouping of filetypes](https://github.com/liquidinvestigations/hoover-snoop2/blob/8a6dbdf1fd1ea56db386628f0559b097cdaa1a61/snoop/data/digests.py#L266)
## Other lists of mime types ###
### Lists of mime types ###
[Mozilla MIME-Type List](https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types)  
[Freeformatter MIME-types List](https://www.freeformatter.com/mime-types-list.html)  
[Git-Hub magic MIME-types List](https://github.com/magic/mime-types)  
[IANA MIME-Types](https://www.iana.org/assignments/media-types/media-types.xhtml)  
### Lists of mime types from forensics tools ###
[Intella List of Filetypes](https://www.vound-software.com/faq)  
[dtsearch List of Filetypes](https://support.dtsearch.com/faq/dts0103.htm)  
[Aleph Filetypes](https://docs.alephdata.org/developers/technical-faq)  

