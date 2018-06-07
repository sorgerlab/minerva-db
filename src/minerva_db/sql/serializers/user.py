from marshmallow_sqlalchemy import ModelSchema
from ..models import User


class UserSchema(ModelSchema):
    class Meta:
        model = User
        exclude = tuple(prop.key
                        for prop in User.__mapper__.iterate_properties
                        if hasattr(prop, 'direction') or prop.key == 'satype')


user_schema = UserSchema()
users_schema = UserSchema(many=True)
