from marshmallow_sqlalchemy import ModelSchema
from ..models import Grant


class GrantSchema(ModelSchema):
    class Meta:
        model = Grant
        include_fk = True
        exclude = tuple(prop.key
                        for prop in Grant.__mapper__.iterate_properties
                        if hasattr(prop, 'direction') or prop.key == 'satype')


grant_schema = GrantSchema()
grants_schema = GrantSchema(many=True)
