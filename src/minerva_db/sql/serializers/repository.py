from marshmallow_sqlalchemy import ModelSchema
from ..models import Repository


class RepositorySchema(ModelSchema):
    class Meta:
        model = Repository
        exclude = tuple(prop.key
                        for prop in Repository.__mapper__.iterate_properties
                        if hasattr(prop, 'direction') or prop.key == 'satype')


repository_schema = RepositorySchema()
repositories_schema = RepositorySchema(many=True)
