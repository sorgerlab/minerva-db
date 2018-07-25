from sqlalchemy import Column, ForeignKey, String, Enum
from sqlalchemy.orm import relationship
from .base import Base


class Membership(Base):
    group_uuid = Column(String(36), ForeignKey('t_group.uuid'),
                        primary_key=True)
    user_uuid = Column(String(36), ForeignKey('t_user.uuid'), primary_key=True)
    membership_type_type = set(['Member', 'Owner'])
    membership_type = Column(
        Enum(*membership_type_type, name='membershiptypes'),
        nullable=False
    )

    group = relationship('Group', back_populates='memberships')
    user = relationship('User', back_populates='memberships')

    def __init__(self, group=None, user=None, membership_type='Member'):
        self.group = group
        self.user = user
        self.membership_type = membership_type
