from sqlalchemy import Column, ForeignKey, String, Boolean
from sqlalchemy.orm import relationship
from .base import Base
from .import_ import Import


class BFU(Base):
    uuid = Column(String(36), primary_key=True)
    name = Column(String(256), unique=True, nullable=False)
    reader = Column(String(256), nullable=False)
    complete = Column(Boolean, nullable=False)
    import_uuid = Column(String(36), ForeignKey(Import.uuid), nullable=False)

    import_ = relationship('Import', back_populates='bfus')
    keys = relationship('Key', back_populates='bfu')
    images = relationship('Image', back_populates='bfu')

    def __init__(self, uuid, name, reader, import_, complete=False):
        self.uuid = uuid
        self.name = name
        self.reader = reader
        self.complete = complete
        self.import_ = import_
