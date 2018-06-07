from marshmallow_sqlalchemy import ModelSchema
from ..models import BFU


class BFUSchema(ModelSchema):
    class Meta:
        model = BFU
        exclude = tuple(prop.key
                        for prop in BFU.__mapper__.iterate_properties
                        if hasattr(prop, 'direction') or prop.key == 'satype')


bfu_schema = BFUSchema()
bfus_schema = BFUSchema(many=True)
