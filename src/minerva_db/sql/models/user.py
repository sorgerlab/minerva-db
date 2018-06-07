from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship
from .subject import Subject


class User(Subject):
    __mapper_args__ = {
        'polymorphic_identity': 'user',
    }

    uuid = Column(String(36), ForeignKey(Subject.uuid), primary_key=True)
    name = Column(String(256), unique=True, nullable=False)
    email = Column(String(256), unique=True, nullable=False)

    groups = relationship('Group', viewonly=True, secondary='t_membership')

    def __init__(self, uuid, name, email):
        self.uuid = uuid
        self.name = name
        self.email = email
