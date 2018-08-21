from sqlalchemy import Column, ForeignKey, String, Boolean
from sqlalchemy.orm import relationship
from .base import Base
from .import_ import Import


class Fileset(Base):
    uuid = Column(String(36), primary_key=True)
    name = Column(String(256), nullable=False)
    reader = Column(String(256), nullable=False)
    reader_software = Column(String(256), nullable=False)
    reader_version = Column(String(256), nullable=False)
    complete = Column(Boolean, nullable=False)
    import_uuid = Column(String(36), ForeignKey(Import.uuid), nullable=False)

    import_ = relationship('Import', back_populates='filesets')
    keys = relationship('Key', back_populates='fileset')
    images = relationship('Image', back_populates='fileset',
                          cascade='all, delete-orphan')

    def __init__(self, uuid, name, reader, reader_software, reader_version,
                 import_, complete=False):
        self.uuid = uuid
        self.name = name
        self.reader = reader
        self.reader_software = reader_software
        self.reader_version = reader_version
        self.complete = complete
        self.import_ = import_
