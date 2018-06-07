from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.ext.associationproxy import association_proxy
from .subject import Subject


class Group(Subject):

    __mapper_args__ = {
        'polymorphic_identity': 'group',
    }

    uuid = Column(String(36), ForeignKey(Subject.uuid), primary_key=True)
    name = Column('name', String(64), unique=True, nullable=False)

    # association proxy of 'memberships' collection to 'user' attribute
    users = association_proxy('memberships', 'user')

    def __init__(self, uuid, name):
        self.uuid = uuid
        self.name = name
