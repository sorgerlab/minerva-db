from sqlalchemy import Column, String, Enum
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship
from .base import Base


class Repository(Base):
    uuid = Column(String(36), primary_key=True)
    name = Column(String(256), unique=True, nullable=False)
    raw_storage_type = set(['Archive', 'Live', 'Destroy'])
    raw_storage = Column(
        Enum(*raw_storage_type, name='rawstoragetypes'),
        nullable=False
    )
    access_type = set(['Private', 'PublicRead', 'PublicWrite'])
    access = Column(
        Enum(*access_type, name='accesstypes'),
        nullable=False
    )

    # association proxy of 'memberships' collection to 'group' attribute
    subjects = association_proxy('grants', 'subject')
    imports = relationship('Import', back_populates='repository',
                           cascade='all, delete-orphan')
    images = relationship('Image', back_populates='repository',
                          cascade='all, delete-orphan')

    def __init__(self, uuid, name, raw_storage=None, access="Private"):
        self.uuid = uuid
        self.name = name
        self.raw_storage = 'Archive' if raw_storage is None else raw_storage
        self.access = access
