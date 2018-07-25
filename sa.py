from sqlalchemy.orm import sessionmaker
from sqlalchemy import event, create_engine
from minerva_db.sql.models import (Base, User, Group, Membership, Repository,
                                   Import, BFU, Image, Key, Grant)
from minerva_db.sql.serializers import repository_schema
from minerva_db.sql.api import Client
import json
# from minerva_db.sql import statements


def setup_db(drop=False):

    # engine = create_engine(
    #     'postgresql://minerva:E3$dcok0,cwDp18@localhost:5432/minerva'
    # )
    engine = create_engine('sqlite:///minerva.db', echo=False)

    # Only drop the database if specified
    # TODO This option is perhaps a bit dangerous
    if drop:
        Base.metadata.drop_all(engine, checkfirst=True)

    # Create the tables if they do not already exist
    Base.metadata.create_all(engine, checkfirst=True)

    # Return the SQLAlchemy session maker
    return sessionmaker(bind=engine)


def main():
    DBSession = setup_db(drop=True)
    session = DBSession(expire_on_commit=True)

    client = Client(session)

    user = User('u1', 'u 1', 'u1@example.com')
    session.add(user)
    session.commit()
    # repository = Repository('r1', 'r 1')
    # grant = Grant(user, repository, permission='Admin')
    # import_ = Import('i1', 'i 1', repository)
    # session.add_all((user, repository, grant, import_))
    # session.commit()

    # repository = session.query(Repository).one()
    # m = repository_schema.dump(repository)

    # print(type(m))
    # print(m)
    # print(json.dumps(repository_schema.dump(repository)))
    repository = client.create_repository(
        'r1',
        'r 1',
        'u1'
    )
    print(type(repository))
    print(repository)

    # print(statements.has_permission(session, user.uuid, 'Repository', repository.uuid).scalar())

    # user = UserFactory()
    # print(user)

    # user1 = User('u1', 'u 1', 'u1@example.com')
    # user2 = User(id='u2', name='u2', email='u2@example.com')
    # group1 = Group('g1', 'g 1')
    # group2 = Group('g2', 'g2')
    # user1.groups.append(group1)
    # user1.groups.append(group2)
    # group1.users.append(user1)
    # session.add_all((user1, group1))
    # print(session.commit())
    #
    # for user in session.query(User).all():
    #     print(user)
    #
    # print('------------------')

    # group = session.query(Group).one()
    # group
    # print(user1.groups)
    # print(user2.groups)
    # print(group1.users)
    # print(group2.users)
    #
    # user = User(id='bob', name='Bob', email='bob@example.com')
    # group = Group(id='lab', name='Lab')
    # user.memberships.append(group)
    # session.add(user)
    # # session.add_all((user, group))
    # session.commit()


if __name__ == '__main__':
    main()
