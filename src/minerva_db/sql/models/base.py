from sqlalchemy.ext.declarative import as_declarative, declared_attr
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

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
