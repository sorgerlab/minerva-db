from marshmallow_sqlalchemy import ModelSchema
from ..models import Group


class GroupSchema(ModelSchema):
    class Meta:
        model = Group
        exclude = tuple(prop.key
                        for prop in Group.__mapper__.iterate_properties
                        if hasattr(prop, 'direction') or prop.key == 'satype')


group_schema = GroupSchema()
groups_schema = GroupSchema(many=True)
