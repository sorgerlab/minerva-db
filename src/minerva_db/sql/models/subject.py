from sqlalchemy import Column, String
from sqlalchemy.orm import relationship
from .base import Base


class Subject(Base):
    satype = Column(String(32))
    __mapper_args__ = {
        'polymorphic_identity': 'subject',
        'polymorphic_on': satype
    }

    uuid = Column(String(36), primary_key=True)

    repositories = relationship('Repository', viewonly=True,
                                secondary='t_grant')
    grants = relationship('Grant', back_populates='subject')
