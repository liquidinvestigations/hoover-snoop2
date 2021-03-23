"""Root views file.

Nothing interesting here, just a global health check endpoint.
"""

from django.http import JsonResponse


def health(request):
    """Always returns HTTP 200 OK with body {"ok":true}."""

    return JsonResponse({
        'ok': True,
    })
