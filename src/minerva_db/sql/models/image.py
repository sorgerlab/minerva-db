from sqlalchemy import Column, ForeignKey, String, Integer
from sqlalchemy.orm import relationship
from .base import Base
from .bfu import BFU


class Image(Base):
    uuid = Column(String(36), primary_key=True)
    name = Column(String(256), nullable=False)
    key = Column(String(1024), nullable=False)
    pyramid_levels = Column(Integer, nullable=False)
    bfu_uuid = Column(String(36), ForeignKey(BFU.uuid), nullable=False)

    bfu = relationship('BFU', back_populates='images')

    def __init__(self, uuid, name, key, pyramid_levels, bfu):
        self.uuid = uuid
        self.name = name
        self.key = key
        self.pyramid_levels = pyramid_levels
        self.bfu = bfu
