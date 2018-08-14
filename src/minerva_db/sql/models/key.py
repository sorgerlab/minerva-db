from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.orm import relationship
from .base import Base
from .fileset import Fileset
from .import_ import Import


class Key(Base):
    key = Column(String(1024), primary_key=True, nullable=False)
    import_uuid = Column(String(36), ForeignKey(Import.uuid), primary_key=True,
                         nullable=False)
    fileset_uuid = Column(String(36), ForeignKey(Fileset.uuid))

    import_ = relationship('Import', back_populates='keys')
    fileset = relationship('Fileset', back_populates='keys')

    def __init__(self, key, import_, fileset=None):
        self.key = key
        self.import_ = import_
        self.fileset = fileset
