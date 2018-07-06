from marshmallow_sqlalchemy import ModelSchema
from ..models import Import


class ImportSchema(ModelSchema):
    class Meta:
        model = Import
        include_fk = True
        exclude = tuple(prop.key
                        for prop in Import.__mapper__.iterate_properties
                        if hasattr(prop, 'direction') or prop.key == 'satype')


import_schema = ImportSchema()
imports_schema = ImportSchema(many=True)
