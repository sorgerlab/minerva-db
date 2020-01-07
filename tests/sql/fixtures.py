import pytest
from sqlalchemy.orm import Session
from sqlalchemy import event, create_engine
from src.minerva_db.sql.api import Client
from src.minerva_db.sql.models import Base
from .factories import (GroupFactory, UserFactory, MembershipFactory,
                        MembershipOwnerFactory, RepositoryFactory,
                        GrantFactory, GrantAdminFactory, ImportFactory,
                        FilesetFactory, ImageFactory, KeyFactory,
                        KeyFilesetFactory, RenderingSettingsFactory)

USE_DOCKER = False

@pytest.fixture(scope='session')
def postgres():
    if not USE_DOCKER:
        yield f'postgresql://postgres:postgres@localhost:5432/postgres'
        return
        
    import docker
    import time

    # Connect to local Docker
    client = docker.from_env()

    # Start the RDF4J Container, mapping an unused host port
    container_id = client.containers.run(
        'postgres',
        detach=True,
        remove=True,
        ports={'5432/tcp': None},
        environment={'POSTGRES_PASSWORD': 'postgres'}
    ).attrs['Id']

    # Wait until the container is running
    while True:
        container = client.containers.get(container_id)
        if container.attrs['State']['Running']:
            break
        else:
            time.sleep(1)

    try:
        # Get the host port that was mapped
        ports = container.attrs['NetworkSettings']['Ports']
        port = int(ports['5432/tcp'][0]['HostPort'])

    except Exception as e:
        container.stop()
        raise e

    # Give the database time to initialise
    # TODO Wait in a more reliable way. Connection seems to be available
    # immediately, but the database is still initialising
    time.sleep(2)

    yield f'postgresql://postgres:postgres@localhost:{port}/postgres'
    container.stop()


@pytest.fixture(scope='session')
def connection(postgres):
    # Connect to the database and create the schema within a transaction
    engine = create_engine(postgres, echo=True)
    connection = engine.connect()
    transaction = connection.begin()
    Base.metadata.create_all(connection)

    # Database Fixtures
    # session = Session(connection)
    # db_fixtures = __db_fixtures(session)
    yield connection
    transaction.rollback()
    connection.close()
    engine.dispose()


@pytest.fixture
def session(connection):
    transaction = connection.begin_nested()
    # TODO Use sessionmaker?
    session = Session(connection, expire_on_commit=True)
    yield session
    session.close()
    transaction.rollback()


@pytest.fixture
def client(session):
    return Client(session)


@pytest.fixture(scope='session')
def statements_base(connection):

    class s():
        def __init__(self):
            self.statements = []

        def before_execute(self, conn, clauseelement, multiparams, params):
            self.statements.append(clauseelement)

        def attach(self):
            event.listen(connection, 'before_execute', self.before_execute)

        def dettach(self):
            event.remove(connection, "before_execute", self.before_execute)
            self.statements = []

    return s()


@pytest.fixture
def statements(statements_base):
    '''Collects the statements executed during a test session.

    Note: It is essential that `statements` be listed _after_ any fixtures
    which cause statements to be executed themselves.

    Args:
        statements_base: Session scoped statement accumulator.

    Yields:
        The list of statements executed.
    '''

    statements_base.attach()
    yield statements_base.statements
    statements_base.dettach()


@pytest.fixture
def db_group(session):
    group = GroupFactory()
    session.add(group)
    session.commit()
    return group


@pytest.fixture
def db_user(session):
    user = UserFactory()
    session.add(user)
    session.commit()
    return user


@pytest.fixture
def db_membership(session):
    membership = MembershipFactory()
    session.add(membership)
    session.commit()
    return membership


@pytest.fixture
def db_ownership(session):
    ownership = MembershipOwnerFactory()
    session.add(ownership)
    session.commit()
    return ownership


@pytest.fixture
def db_users(session):
    users = UserFactory.create_batch(2)
    session.add_all(users)
    session.commit()
    return users


@pytest.fixture
def db_repository(session):
    repository = RepositoryFactory()
    session.add(repository)
    session.commit()
    return repository


@pytest.fixture
def db_import(session):
    import_ = ImportFactory()
    session.add(import_)
    session.commit()
    return import_


@pytest.fixture
def db_import_with_keys(session):
    import_ = ImportFactory()
    session.add(import_)
    keys = KeyFactory.create_batch(5, import_=import_)
    session.add_all(keys)
    session.commit()
    return import_


@pytest.fixture
def db_fileset(session):
    fileset = FilesetFactory()
    session.add(fileset)
    session.commit()
    return fileset


@pytest.fixture
def db_image(session):
    image = ImageFactory()
    session.add(image)
    session.commit()
    return image

@pytest.fixture
def db_rendering_settings(session):
    rendering_settings = RenderingSettingsFactory()
    session.add(rendering_settings)
    session.commit()
    return rendering_settings

@pytest.fixture
def user_granted_repository(session):
    grant = GrantAdminFactory()
    session.add(grant)
    session.commit()
    return grant.repository


@pytest.fixture
def many_user_granted_repository(session, user_granted_repository):
    users = GrantFactory.create_batch(
        2, repository=user_granted_repository)
    session.add_all(users)
    session.commit()
    return user_granted_repository


@pytest.fixture
def user_granted_read_hierarchy(session):
    user = UserFactory()
    repository = RepositoryFactory()
    grant = GrantFactory(subject=user, repository=repository)
    import_ = ImportFactory(repository=repository)
    fileset = FilesetFactory(import_=import_)
    image = ImageFactory(fileset=fileset)
    key = KeyFilesetFactory(import_=import_, fileset=fileset)

    session.add_all([user, repository, grant, import_, fileset, key, image])
    session.commit()
    return {
        'user': user,
        'user_uuid': user.uuid,
        'repository': repository,
        'repository_uuid': repository.uuid,
        'grant': grant,
        'import_': import_,
        'import_uuid': import_.uuid,
        'fileset': fileset,
        'fileset_uuid': fileset.uuid,
        'key': key,
        'image': image,
        'image_uuid': image.uuid
    }


@pytest.fixture
def group_granted_read_hierarchy(session):
    user = UserFactory()
    group = GroupFactory()
    membership = MembershipFactory(group=group, user=user,
                                   membership_type='Member')
    repository = RepositoryFactory()
    grant = GrantFactory(subject=group, repository=repository)
    import_ = ImportFactory(repository=repository)
    fileset = FilesetFactory(import_=import_)
    key = KeyFilesetFactory(import_=import_, fileset=fileset)
    image = ImageFactory(fileset=fileset)

    session.add_all([user, group, membership, repository, grant, import_,
                     fileset, key, image])
    session.commit()
    return {
        'user': user,
        'user_uuid': user.uuid,
        'group': group,
        'group_uuid': group.uuid,
        'membership': membership,
        'repository': repository,
        'repository_uuid': repository.uuid,
        'grant': grant,
        'import_': import_,
        'import_uuid': import_.uuid,
        'fileset': fileset,
        'fileset_uuid': fileset.uuid,
        'key': key,
        'image': image,
        'image_uuid': image.uuid
    }
