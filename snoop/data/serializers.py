from rest_framework import serializers

from . import models


class DocumentUserTagSerializer(serializers.ModelSerializer):
    blob = serializers.CharField(source='digest.blob.pk', read_only=True)

    class Meta:
        model = models.DocumentUserTag
        fields = ['id', 'blob', 'field', 'user', 'public', 'tag', 'date_modified', 'date_created']
        read_only_fields = ['id', 'digest_id', 'field', 'user', 'blob', 'date_modified', 'date_created']

    # def to_representation(self, instance):
    #    # do nothing special
    #    return super().to_representation(instance)

    # def to_internal_value(self, validated_data):
    #    # set digest_id, user from context
    #    data = dict(validated_data)
    #    assert 'public' in data
    #    data['user'] = self.context['user']
    #    data['digest_id'] = self.context['digest_id']
    #    return super().to_internal_value(data)

    def create(self, validated_data):
        data = dict(validated_data)
        assert 'public' in data
        data['user'] = self.context['user']
        data['digest_id'] = self.context['digest_id']
        return super().create(data)

    def update(self, instance, validated_data):
        data = dict(validated_data)
        assert 'public' in data
        data['user'] = self.context['user']
        data['digest_id'] = self.context['digest_id']
        return super().update(instance, data)
