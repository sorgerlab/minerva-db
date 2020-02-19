from marshmallow_sqlalchemy import ModelSchema
from ..models import RenderingSettings


class RenderingSettingsSchema(ModelSchema):
    class Meta:
        model = RenderingSettings
        include_fk = True
        exclude = tuple(prop.key
                        for prop in RenderingSettings.__mapper__.iterate_properties
                        if hasattr(prop, 'direction') or prop.key == 'satype')


rendering_settings_schema = RenderingSettingsSchema(many=True)
