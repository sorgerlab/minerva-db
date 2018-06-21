from sqlalchemy import create_engine, Column, String, ForeignKey, Enum
from sqlalchemy.orm import (relationship, backref, sessionmaker,
                            with_polymorphic)
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.inspection import inspect
from sqlalchemy.orm.state import InstanceState
from stringcase import snakecase


@as_declarative()
class Base():

    @declared_attr
    def __tablename__(cls):
        name = snakecase(cls.__name__).lstrip('_')
        return f't_{name}'

    def __repr__(self):
        inspection = inspect(self)

        if type(inspection) is InstanceState:
            pks = [pk.name for pk in inspection.mapper.primary_key]
            attrs = [attr for attr in inspection.attrs if attr.key in pks]
        else:
            return 'Not an InstanceState!'

        # Order lexically, but always put id first
        attrs = sorted(attrs,
                       key=lambda attr: '.' if attr.key is 'id' else attr.key)
        pretty_attrs = [f'{attr.key}: {attr.value}' for attr in attrs]
        pretty_attrs = ', '.join(pretty_attrs)
        return '<{0}({1})>'.format(type(self).__name__, pretty_attrs)


class Subject(Base):
    satype = Column(String(32))
    __mapper_args__ = {
        'polymorphic_identity': 'subject',
        'polymorphic_on': satype
    }

    id = Column(String(36), primary_key=True)


class Membership(Base):
    user_id = Column(String(36), ForeignKey('t_user.id'), primary_key=True)
    group_id = Column(String(36), ForeignKey('t_group.id'), primary_key=True)
    membership_type_type = set(['Member', 'Owner'])
    membership_type = Column(
        Enum(*membership_type_type, name='membershiptypes'),
        nullable=False
    )

    # bidirectional attribute/collection of 'user'/'user_keywords'
    # user = relationship('User', backref=backref('memberships',
    #                                             cascade='all, delete-orphan'))
    group = relationship('Group', backref=backref('memberships',
                                                  cascade='all, delete-orphan'))

    # reference to the 'Group' object
    # group = relationship('Group')
    user = relationship('User')

    def __init__(self, group=None, user=None, membership_type='Member'):
        self.user = user
        self.group = group
        self.membership_type = membership_type


class User(Subject):
    __mapper_args__ = {
        'polymorphic_identity': 'user',
    }

    id = Column(String(36), ForeignKey(Subject.id), primary_key=True)
    name = Column(String(64), unique=True, nullable=False)
    email = Column(String(256), unique=True, nullable=False)

    # association proxy of 'user_keywords' collection
    # to 'group' attribute
    # groups = association_proxy('memberships', 'group')

    def __init__(self, id, name, email):
        self.id = id
        self.name = name
        self.email = email


class Group(Subject):

    __mapper_args__ = {
        'polymorphic_identity': 'group',
    }

    id = Column(String(36), ForeignKey(Subject.id), primary_key=True)
    name = Column('name', String(64), unique=True, nullable=False)

    users = association_proxy('memberships', 'user')

    def __init__(self, id, name):
        self.id = id
        self.name = name


SubjectWithPolymorphic = with_polymorphic(Subject, [User, Group])


engine = create_engine('sqlite:///test.db')

Base.metadata.drop_all(engine, checkfirst=True)
Base.metadata.create_all(engine, checkfirst=True)

# Return the SQLAlchemy session maker
DBSession = sessionmaker(bind=engine)
session = DBSession()

user = User('u1', 'u1', 'u1@example.com')
group = Group('g1', 'g1')
group.users.append(user)
session.add(group)
session.commit()
print(user.groups)
