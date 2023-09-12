"""The Common Data app is used for making tables on the "default" database.

This is needed because we route all models from the "data" app into a
collection database. So we will put the common models, shared between all
collections, in here."""

from django.apps import AppConfig


class CommonDataConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'snoop.common_data'
