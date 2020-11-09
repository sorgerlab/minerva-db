from sqlalchemy import Column, ForeignKey, String, Integer, Boolean
from sqlalchemy.orm import relationship
from .base import Base
from .fileset import Fileset
from .repository import Repository


class Image(Base):
    uuid = Column(String(36), primary_key=True)
    name = Column(String(256), nullable=False)
    pyramid_levels = Column(Integer, nullable=False)
    deleted = Column(Boolean, nullable=False)
    fileset_uuid = Column(String(36), ForeignKey(Fileset.uuid), nullable=True)
    repository_uuid = Column(String(36), ForeignKey(Repository.uuid), nullable=True)
    format = Column(String(256), nullable=True)
    compression = Column(String(256), nullable=True)
    tile_size = Column(Integer, nullable=False)
    rgb = Column(Boolean, nullable=False)

    fileset = relationship('Fileset', back_populates='images')
    repository = relationship('Repository', back_populates='images')
    rendering_settings = relationship('RenderingSettings', back_populates='image',
                          cascade='all, delete-orphan')

    def __init__(self, uuid, name, pyramid_levels, format, compression, tile_size, fileset, repository, rgb=False):
        self.uuid = uuid
        self.name = name
        self.pyramid_levels = pyramid_levels
        self.fileset = fileset
        self.repository = repository
        self.deleted = False
        self.format = format
        self.compression = compression
        self.tile_size = tile_size
        self.rgb = rgb
