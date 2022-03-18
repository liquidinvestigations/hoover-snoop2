"""Here we define the schema for our GraphQL endpoints. There is one endpoint for each collection.

We use graphene_django to make the django models available to the GraphQL queries.
"""

from graphene_django import DjangoObjectType
import graphene
from .models import Entity as EntityModel
from .models import EntityType as EntityTypeModel
from .models import Digest as DigestModel
from .models import EntityHit as EntityHitModel


class Entity(DjangoObjectType):
    """Graphene class for the entity model from django."""
    class Meta:
        model = EntityModel


class EntityType(DjangoObjectType):
    """Graphene class for the Entity Type model from django."""
    class Meta:
        model = EntityTypeModel


class Digest(DjangoObjectType):
    """Graphene class for the digest model from django."""
    class Meta:
        model = DigestModel


class EntityHit(DjangoObjectType):
    """Graphene class for the Entity Hit model from django."""
    class Meta:
        model = EntityHitModel


class Query(graphene.ObjectType):
    """Define the schema for GraphQL queries."""
    entities = graphene.List(Entity)
    entityTypes = graphene.List(EntityType)
    digests = graphene.List(Digest)
    entityHits = graphene.List(EntityHit)

    def resolve_entities(self, info):
        """Define what should be returned when querying for entities."""
        return EntityModel.objects.all()

    def resolve_entityTypes(self, info):
        """Define what should be returned when querying for entity type."""
        return EntityTypeModel.objects.all()

    def resolve_digests(self, info):
        """Define what should be returned when querying for digests."""
        return DigestModel.objects.all()

    def resolve_entityHits(self, info):
        """Define what should be returned when querying for entity Hits."""
        return EntityHitModel.objects.all()


schema = graphene.Schema(query=Query)
