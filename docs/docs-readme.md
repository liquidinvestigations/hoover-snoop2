# The Documentation System

We're using MKdocs with various plugins so everything is auto-generated from docstrings inside the code.
We use Google docstring style [^4] in python code.

Take a look at the source codes for [this file][snoop.data.models] and [this
other file][snoop.defaultsettings] for examples of how that looks like in the
code.

Cross references are made with full qualified object names [^5]. Click the
"Edit" button at the top of the page to see how that looks like.

Linking to GitHub can also be done: #66, hoover-search#66, CRJI/EIC#666,
github:liquidinvestigations/node#66 (for cross-provider URLs). These are
auto-linked with no markdown needed. Getting a link is avoided by using
backticks: `#66`.

## Issues and their work-arounds

There's still some work to do:

- There's no way to control capitalization in `MkDocs` [^1] so we added our own
  CSS to make everything lowercase on the left side of the screen.
- For Django Rest Framework, deactivating some parts of the internal field
  parser of `mkdocstrings` is required [^2].
- There is no out-of-the-box configuration for mocking out modules (like Sphinx
  has [^3]) so we had to roll our own. The full path of the mocked object must
  be provided, not only just top modules - maybe we could install Sphinx
  and use its code for mocking?
- Complete inheritance tree must be accessible for a class to show up in the
  docs. So we can't mock out `Django`, `rest_framework`, or any other library that
  we base our Classes from, or that we use at the top level (import level).
- The footnotes don't work well with titles [^6].
- The footnotes don't renumber themselves in the correct order [^7].
- Since we use separate requirements files for building docs (to ease setting
  up the build process for readthedocs), the versions for the various libraries
  that we do install (only `django` and `rest_framework` and their
  dependencies) will be out of sync from the main code repository.

[^1]: <https://github.com/mkdocs/mkdocs/issues/1289#issuecomment-331021585>
[^2]: <https://github.com/mkdocstrings/mkdocstrings/issues/141>
[^3]: <https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#confval-autodoc_mock_imports>
[^4]: <https://google.github.io/styleguide/pyguide.html>
[^5]: <https://mkdocstrings.github.io/usage/#cross-references>
[^6]: <https://github.com/Python-Markdown/markdown/issues/660>
[^7]: <https://github.com/Python-Markdown/markdown/issues/1117>
