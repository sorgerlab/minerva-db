from sqlalchemy import Column, ForeignKey, String, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from .base import Base
from .image import Image


class Channel:
    def __init__(self, id:int, label:str, color:str, min:float, max:float):
        self.id = id
        self.label = label
        self.color = color
        self.min = min
        self.max = max

    def as_dict(self):
        return {
            "id": self.id,
            "label": self.label,
            "color": self.color,
            "min": self.min,
            "max": self.max
        }


class RenderingSettings(Base):
    uuid = Column(String(36), primary_key=True)
    image_uuid = Column(String(36), ForeignKey(Image.uuid), nullable=False, index=True)
    label = Column(String(255))
    channels = Column(JSONB, nullable=False)

    image = relationship('Image', back_populates='rendering_settings')

    def __init__(self, uuid, image, channels, label=None):
        self.uuid = uuid
        self.image = image
        self.label = label
        self.channels = channels





