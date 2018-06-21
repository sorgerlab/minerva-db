from marshmallow_sqlalchemy import ModelSchema
from ..models import Membership


class MembershipSchema(ModelSchema):
    class Meta:
        model = Membership
        include_fk = True
        exclude = tuple(prop.key
                        for prop in Membership.__mapper__.iterate_properties
                        if hasattr(prop, 'direction') or prop.key == 'satype')


membership_schema = MembershipSchema()
memberships_schema = MembershipSchema(many=True)
