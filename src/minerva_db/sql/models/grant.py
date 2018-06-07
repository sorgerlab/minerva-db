from sqlalchemy import Column, ForeignKey, String, Enum
from sqlalchemy.orm import backref, relationship
from .base import Base


class Grant(Base):
    subject_uuid = Column(String(36), ForeignKey('t_subject.uuid'),
                          primary_key=True)
    repository_uuid = Column(String(36), ForeignKey('t_repository.uuid'),
                             primary_key=True)
    permission_type = set(['Read', 'Write', 'Admin'])
    permission = Column(Enum(*permission_type, name='permissions'),
                        nullable=False)

    repository = relationship('Repository',
                              backref=backref('grants',
                                              cascade='all, delete-orphan'))
    subject = relationship('Subject', back_populates='grants')

    def __init__(self, subject=None, repository=None, permission='Read'):
        self.subject = subject
        self.repository = repository
        self.permission = permission
