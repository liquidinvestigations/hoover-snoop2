# On the documentation

We're using MKdocs with various plugins so everything is auto-generated from docstrings inside the code.

Take a look at [this file][snoop.data.models] and [this other file][snoop.defaultsettings] for examples of how that looks in the code.
Take a look

There's still some work to do:

- There's no way to control capitalization in `MkDocs` [^1] so we added our own
  CSS to make everything lowercase on the left side of the screen.
- For Django Rest Framework, deactivating some parts of the internal field
  parser of `mkdocstrings` is required [^2].
- There is no out-of-the-box configuration for mocking out modules (like Sphinx
  has: [^3]) so we had to roll our own. The full path of the mocked object must
  be provided, not only just top modules - maybe we could use Sphinx'
  functionality for mocking?
- Complete inheritance tree must be accessible for a class to show up in the
  docs. So we can't mock out `Django`, `rest_framework`, or any other library that
  we base our Classes from, or that we use at the top level (import level).

[^1]: <https://github.com/mkdocs/mkdocs/issues/1289#issuecomment-331021585>
[^2]: <https://github.com/mkdocstrings/mkdocstrings/issues/141>
[^3]: <https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#confval-autodoc_mock_imports>
