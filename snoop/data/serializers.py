from rest_framework import serializers

from . import models


class DocumentUserTagSerializer(serializers.ModelSerializer):
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
