"""Django Rest Framwork serializer for the Tags API.
"""

from rest_framework import serializers

from . import models


class DocumentUserTagSerializer(serializers.ModelSerializer):
    """Serializer for the Tags API.

    Combines fields from the table with other fields from the URL path. Since this URL path is private
    between the backend services, we use it to store pertinent information too (e.g. user interacting with
    tags).

    All fields are read-only except "public" and "tag". The UI doesn't edit "tag", so we may remove editing
    it in the future.
    """

    blob = serializers.CharField(source='digest.blob.pk', read_only=True)

    class Meta:
        model = models.DocumentUserTag
        fields = [
            'blob',
            'date_created',
            'date_indexed',
            'date_modified',
            'field',
            'id',
            'public',
            'tag',
            'user',
        ]
        read_only_fields = [
            'blob',
            'date_created',
            'date_indexed',
            'date_modified',
            'digest_id',
            'field',
            'id',
            'user',
            'uuid',
        ]

    def create(self, validated_data):
        data = dict(validated_data)
        data['user'] = self.context['user']
        data['uuid'] = self.context['uuid']
        data['digest_id'] = self.context['digest_id']
        return super().create(data)

    def update(self, instance, validated_data):
        data = dict(validated_data)
        data['user'] = self.context['user']
        data['uuid'] = self.context['uuid']
        data['digest_id'] = self.context['digest_id']
        return super().update(instance, data)
