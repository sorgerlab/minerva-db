from sqlalchemy import Column, ForeignKey, String, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from .base import Base
from .image import Image


class Channel:
    def __init__(self, index:int, name:str, color:str, min:float, max:float):
        self.index = index
        self.name = name
        self.color = color
        self.min = min
        self.max = max

    def as_dict(self):
        return {
            "index": self.index,
            "name": self.name,
            "color": self.color,
            "min": self.min,
            "max": self.max
        }


class ChannelGroup:

    def __init__(self, name=None):
        self.name = name
        self.channels = {}

    def add(self, channel:Channel):
        self.channels[channel.index] = channel

    def as_dict(self):
        obj = {"channels": {}}
        for key, channel in self.channels.items():
            obj["channels"][channel.index] = channel.as_dict()
        return obj


class RenderingSettings(Base):
    uuid = Column(String(36), primary_key=True)
    image_uuid = Column(String(36), ForeignKey(Image.uuid), nullable=False, index=True)
    name = Column(String(255))
    channel_group = Column(JSONB, nullable=False)

    image = relationship('Image', back_populates='rendering_settings')

    def __init__(self, uuid, image, channel_group: ChannelGroup):
        self.uuid = uuid
        self.image = image
        self.name = channel_group.name
        self.channel_group = {"channels": {}}
        for channel in channel_group.channels.values():
            self.channel_group["channels"][channel.index] = channel.as_dict()

    def get_channel_group(self) -> ChannelGroup:
        channel_group = ChannelGroup(self.name)

        for name, obj in self.channel_group["channels"].items():
            channel_group.add(Channel(obj["index"], obj["name"], obj["color"], obj["min"], obj["max"]))

        return channel_group




