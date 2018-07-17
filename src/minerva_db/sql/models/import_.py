from sqlalchemy import Column, ForeignKey, String, Boolean
from sqlalchemy.orm import relationship
from .base import Base
from .repository import Repository


class Import(Base):
    uuid = Column(String(36), primary_key=True)
    name = Column(String(256), unique=True, nullable=False)
    complete = Column(Boolean, nullable=False)
    repository_uuid = Column(String(36), ForeignKey(Repository.uuid),
                             nullable=False)

    repository = relationship('Repository', back_populates='imports')
    bfus = relationship('BFU', back_populates='import_',
                        cascade='all, delete-orphan')
    keys = relationship('Key', back_populates='import_',
                        cascade='all, delete-orphan')

    def __init__(self, uuid, name, repository, complete=False):
        self.uuid = uuid
        self.name = name
        self.repository = repository
        self.complete = complete
