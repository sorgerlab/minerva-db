from sqlalchemy import Column, ForeignKey, String, Enum
from sqlalchemy.orm import backref, relationship
from .base import Base


class Membership(Base):
    user_uuid = Column(String(36), ForeignKey('t_user.uuid'), primary_key=True)
    group_uuid = Column(String(36), ForeignKey('t_group.uuid'),
                        primary_key=True)
    membership_type_type = set(['Member', 'Owner'])
    membership_type = Column(
        Enum(*membership_type_type, name='membershiptypes'),
        nullable=False
    )

    group = relationship('Group',
                         backref=backref('memberships',
                                         cascade='all, delete-orphan'))
    user = relationship('User')

    def __init__(self, user=None, group=None, membership_type='Member'):
        self.user = user
        self.group = group
        self.membership_type = membership_type
