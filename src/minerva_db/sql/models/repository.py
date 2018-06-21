from sqlalchemy import Column, String
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship
from .base import Base


class Repository(Base):
    uuid = Column(String(36), primary_key=True)
    name = Column(String(256), unique=True, nullable=False)

    # association proxy of 'memberships' collection to 'group' attribute
    subjects = association_proxy('grants', 'subject')
    imports = relationship('Import', back_populates='repository')

    def __init__(self, uuid, name):
        self.uuid = uuid
        self.name = name
