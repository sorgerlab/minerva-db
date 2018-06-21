from marshmallow_sqlalchemy import ModelSchema
from ..models import Image


class ImageSchema(ModelSchema):
    class Meta:
        model = Image
        exclude = tuple(prop.key
                        for prop in Image.__mapper__.iterate_properties
                        if hasattr(prop, 'direction') or prop.key == 'satype')


image_schema = ImageSchema()
images_schema = ImageSchema(many=True)
