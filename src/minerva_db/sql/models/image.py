from sqlalchemy import Column, ForeignKey, String, Integer
from sqlalchemy.orm import relationship
from .base import Base
from .fileset import Fileset


class Image(Base):
    uuid = Column(String(36), primary_key=True)
    name = Column(String(256), nullable=False)
    pyramid_levels = Column(Integer, nullable=False)
    fileset_uuid = Column(String(36), ForeignKey(Fileset.uuid), nullable=False)

    fileset = relationship('Fileset', back_populates='images')

    def __init__(self, uuid, name, pyramid_levels, fileset):
        self.uuid = uuid
        self.name = name
        self.pyramid_levels = pyramid_levels
        self.fileset = fileset
