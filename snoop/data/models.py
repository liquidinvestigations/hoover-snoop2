from django.db import models


class Collection(models.Model):
    name = models.CharField(max_length=128, unique=True)
    path = models.CharField(max_length=4096)
