from sqlalchemy import Column, ForeignKey, String, Integer
from sqlalchemy.orm import relationship
from .base import Base
from .fileset import Fileset
from .repository import Repository


class Image(Base):
    uuid = Column(String(36), primary_key=True)
    name = Column(String(256), nullable=False)
    pyramid_levels = Column(Integer, nullable=False)
    fileset_uuid = Column(String(36), ForeignKey(Fileset.uuid), nullable=True)
    repository_uuid = Column(String(36), ForeignKey(Repository.uuid), nullable=True)

    fileset = relationship('Fileset', back_populates='images')
    repository = relationship('Repository', back_populates='images')
    rendering_settings = relationship('RenderingSettings', back_populates='image',
                          cascade='all, delete-orphan')

    def __init__(self, uuid, name, pyramid_levels, fileset, repository):
        self.uuid = uuid
        self.name = name
        self.pyramid_levels = pyramid_levels
        self.fileset = fileset
        self.repository = repository
