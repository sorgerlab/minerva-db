from sqlalchemy.orm import with_polymorphic

from .base import Base
from .membership import Membership
from .grant import Grant
from .subject import Subject
from .group import Group
from .user import User
from .repository import Repository
from .import_ import Import
from .fileset import Fileset
from .image import Image
from .key import Key
from .renderingsettings import RenderingSettings, ChannelGroup, Channel


# class Obj(Base):
#     ''' Generic object for use in permissions and other generic object
#         operations
#     '''
#
#     satype = Column(String(30))
#     __mapper_args__ = {
#         'polymorphic_identity': 'obj',
#         'polymorphic_on': satype
#     }
#
#     id = Column(String(36), primary_key=True)
#
#     grants = relationship('Grant',
#                           back_populates='obj',
#                           cascade='save-update, merge, delete')

SubjectWithPolymorphic = with_polymorphic(Subject, [User, Group], flat=True)


__all__ = ['Base', 'Membership', 'Grant', 'Subject', 'Group', 'User',
           'Repository', 'Import', 'Fileset', 'Image', 'Key', 'RenderingSettings',
           'SubjectWithPolymorphic']
