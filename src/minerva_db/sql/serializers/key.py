from marshmallow_sqlalchemy import ModelSchema
from ..models import Key


class KeySchema(ModelSchema):
    class Meta:
        model = Key
        exclude = tuple(prop.key
                        for prop in Key.__mapper__.iterate_properties
                        if hasattr(prop, 'direction') or prop.key == 'satype')


key_schema = KeySchema()
keys_schema = KeySchema(many=True)
