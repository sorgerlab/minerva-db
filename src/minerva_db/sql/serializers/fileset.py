from marshmallow_sqlalchemy import ModelSchema
from ..models import Fileset


class FilesetSchema(ModelSchema):
    class Meta:
        model = Fileset
        include_fk = True
        exclude = tuple(prop.key
                        for prop in Fileset.__mapper__.iterate_properties
                        if hasattr(prop, 'direction') or prop.key == 'satype')


fileset_schema = FilesetSchema()
filesets_schema = FilesetSchema(many=True)
