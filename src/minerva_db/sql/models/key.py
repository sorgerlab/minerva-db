from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship
from .base import Base
from .bfu import BFU
from .import_ import Import


class Key(Base):
    key = Column(String(1024), primary_key=True, nullable=False)
    import_uuid = Column(String(36), ForeignKey(Import.uuid), nullable=False)
    bfu_uuid = Column(String(36), ForeignKey(BFU.uuid))

    import_ = relationship('Import', back_populates='keys')
    bfu = relationship('BFU', back_populates='keys')

    def __init__(self, key, import_, bfu=None):
        self.key = key
        self.import_ = import_
        self.bfu = bfu
