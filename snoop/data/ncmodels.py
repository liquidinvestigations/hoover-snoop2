from django.db import models


class NextcloudCollection(models.Model):
    """Model for storing nextcloud collection metadata."""

    name = models.CharField(max_length=256, unique=True)
    user = models.CharField(max_length=256)
    url = models.CharField(max_length=256, unique=True)
    password = models.CharField(max_length=256)
    initialized = models.BooleanField(default=False)
