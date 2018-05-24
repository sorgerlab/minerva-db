import pytest
from minerva_db.sparql import Client


def assert_rowsets_equal(a, b):

    assert len(a) == len(b)

    if len(a) == 0:
        return

    def kfunc(row):
        # Get the keys used in the row, ordered
        keys = sorted(row.keys())

        # Yield tuples with the keys and their value
        return [(key, row[key]) for key in keys]

    a = sorted(a, key=kfunc)
    b = sorted(b, key=kfunc)

    assert a == b


@pytest.fixture(scope='session')
def prefix():
    import os
    import pkg_resources
    return pkg_resources.resource_string(
        'minerva_db.sparql',
        os.path.join('schema', 'prefix.rq')
    ).decode('utf-8')


@pytest.fixture(scope='session')
def schema():
    import os
    import pkg_resources
    return pkg_resources.resource_string(
        'minerva_db.sparql',
        os.path.join('schema', 'schema.rq')
    ).decode('utf-8')


@pytest.fixture(scope='session')
def db():
    import docker
    import requests
    import time

    # Connect to local Docker
    client = docker.from_env()

    # Start the RDF4J Container, mapping an unused host port
    container_id = client.containers.run(
        'yyz1989/rdf4j',
        detach=True,
        remove=True,
        ports={'8080/tcp': None}
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
        port = int(ports['8080/tcp'][0]['HostPort'])

        # Wait until tomcat is ready
        while True:
            try:
                r = requests.head('http://localhost:{}'.format(port))
                if r.status_code == 200:
                    break
                else:
                    time.sleep(1)
            except requests.ConnectionError:
                time.sleep(1)

        # Initialise the database
        input = 'create memory\ntest\nTest store\n\n\n\n'
        server = 'http://localhost:8080/rdf4j-server'
        container.exec_run([
            'sh',
            '-c',
            'echo "{}" | ${{RDF4J_DATA}}/../bin/console.sh -q -s {}'.format(
                input,
                server
            )
        ])
    except Exception as e:
        container.stop()
        raise e

    yield 'http://localhost:{}/rdf4j-server/repositories/test'.format(port)
    container.stop()


@pytest.fixture(scope='class')
def client(db, prefix, schema):
    client = Client(db)
    client._connection().update(prefix + schema)
    yield client
    client._connection().update('DELETE WHERE { ?s ?p ?o }')


@pytest.fixture
def user_bob():
    return {
        'uuid': 'bob',
        'name': 'Lonesome Bob',
        'email': 'bob@example.com'
    }


@pytest.fixture
def user_bill():
    return {
        'uuid': 'bill',
        'name': 'Big Bob',
        'email': 'bill@example.com'
    }


@pytest.fixture(params=['user_bob', 'user_bill'])
def users(request):
    return request.getfixturevalue(request.param)


@pytest.fixture
def group_somelab():
    return {
        'uuid': 'somelab',
        'name': 'The Some lab'
    }


@pytest.fixture
def repository_repo1():
    return {
        'uuid': 'repo1',
        'name': 'Repository One'
    }


@pytest.fixture
def import_import1():
    return {
        'uuid': 'import1',
        'name': 'Import One',
        'key': 's3://raw/import1',
        'complete': False
    }


@pytest.fixture
def files_dv1():
    return [
        's3://raw/import1/bfu1/bfu1.dv',
        's3://raw/import1/bfu1/bfu1.dv.log'
    ]


@pytest.fixture
def bfu_dv1():
    return {
        'uuid': 'bfu1',
        'name': 'BFU One',
        'reader': 'loci.formats.in.DeltavisionReader'
    }


@pytest.fixture
def image_dv1():
    return {
        'uuid': 'image1',
        'name': 'bfu1.dv',
        'key': 's3://tiles/image1',
        'pyramidLevels': 8
    }


class TestIndividual():

    @pytest.fixture(autouse=True)
    def transact(self, client, prefix, schema):
        client._connection().update(prefix + schema)
        yield
        # TODO Investigate if a better way of doing transactions with the
        # SPARQL endpoint is possible?
        client._connection().update('DELETE WHERE { ?s ?p ?o }')

    def test_connection(self, client):
        client._connection().query('''
            SELECT ?s ?p ?o
            WHERE { ?s ?p ?o . }
        ''')

    def test_init_db(self, client):
        expected_header, expected_rows = client._connection().query('''
            SELECT ?s ?p ?o
            WHERE { ?s ?p ?o . }
        ''')

        client._init_db()

        result_header, result_rows = client._connection().query('''
            SELECT ?s ?p ?o
            WHERE { ?s ?p ?o . }
        ''')

        assert_rowsets_equal(expected_rows, result_rows)

    def test_init_db_extra(self, client, prefix):
        expected_header, expected_rows = client._connection().query('''
            SELECT ?s ?p ?o
            WHERE { ?s ?p ?o . }
        ''')

        triple = ('http://test#test0', 'http://test#test1',
                  'http://test#test2')

        expected_rows += [{'s': triple[0], 'p': triple[1], 'o': triple[2]}]

        extra = '''
            INSERT DATA {
                <%s> <%s> <%s> .
            }
        ''' % triple

        client._init_db(extra)

        result_header, result_rows = client._connection().query('''
            SELECT ?s ?p ?o
            WHERE { ?s ?p ?o . }
        ''')

        assert_rowsets_equal(expected_rows, result_rows)

    def test_user(self, client, user_bob):
        keys = {'name', 'email'}
        expected = {k: user_bob[k] for k in user_bob.keys() & keys}

        client.create_user(**user_bob)

        result = client.describe_user(user_bob['uuid'])
        assert expected == result

    def test_user_nonexistant(self, client, user_bob):
        with pytest.raises(ValueError):
            client.describe_user(user_bob['uuid'])

    def test_group(self, client, group_somelab, user_bob):
        keys = {'name'}
        expected = {k: group_somelab[k] for k in group_somelab.keys() & keys}

        client.create_user(**user_bob)
        client.create_group(user=user_bob['uuid'], **group_somelab)

        result = client.describe_group(group_somelab['uuid'])
        assert expected == result

    def test_group_nonexistant(self, client, group_somelab):
        with pytest.raises(ValueError):
            client.describe_group(group_somelab['uuid'])

    def test_repository(self, client, repository_repo1, user_bob):
        keys = {'name'}
        expected = {k: repository_repo1[k]
                    for k in repository_repo1.keys() & keys}

        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)

        result = client.describe_repository(repository_repo1['uuid'])
        assert expected == result

    def test_repository_nonexistant(self, client, repository_repo1):
        with pytest.raises(ValueError):
            client.describe_repository(repository_repo1['uuid'])

    def test_import(self, client, import_import1, repository_repo1, user_bob):
        keys = {'name', 'key', 'complete'}
        expected = {
            **{k: import_import1[k] for k in import_import1.keys() & keys},
            'repository': repository_repo1['uuid']
        }

        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])

        result = client.describe_import(import_import1['uuid'])
        assert expected == result

    def test_import_nonexistant(self, client, import_import1):
        with pytest.raises(ValueError):
            client.describe_import(import_import1['uuid'])

    def test_files_in_import(self, client, files_dv1, import_import1,
                             repository_repo1, user_bob):
        expected = [{'key': key} for key in files_dv1]

        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])
        client.add_files_to_import(files_dv1, import_=import_import1['uuid'])

        result = client.list_files_in_import(import_import1['uuid'])

        assert_rowsets_equal(expected, result)

    def test_bfu(self, client, bfu_dv1, files_dv1, import_import1,
                 repository_repo1, user_bob):
        keys = {'name', 'key', 'reader'}
        expected = {
            **{k: bfu_dv1[k] for k in bfu_dv1.keys() & keys},
            'import': import_import1['uuid'],
            'entrypoint': files_dv1[0]
        }

        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])
        client.add_files_to_import(files_dv1, import_=import_import1['uuid'])
        client.create_bfu(import_=import_import1['uuid'], keys=files_dv1,
                          **bfu_dv1)

        result = client.describe_bfu(bfu_dv1['uuid'])

        assert expected == result

    def test_bfu_nonexistant(self, client, bfu_dv1):
        with pytest.raises(ValueError):
            client.describe_user(bfu_dv1['uuid'])

    def test_files_in_bfu(self, client, bfu_dv1, files_dv1, import_import1,
                          repository_repo1, user_bob):
        expected = [{'key': key, 'entrypoint': False} for key in files_dv1]
        expected[0]['entrypoint'] = True

        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])
        client.add_files_to_import(files_dv1, import_=import_import1['uuid'])
        client.create_bfu(import_=import_import1['uuid'], keys=files_dv1,
                          **bfu_dv1)

        result = client.list_files_in_bfu(bfu_dv1['uuid'])

        assert_rowsets_equal(expected, result)

    def test_image(self, client, image_dv1, bfu_dv1, files_dv1, import_import1,
                   repository_repo1, user_bob):
        keys = {'name', 'key', 'pyramidLevels'}
        expected = {
            **{k: image_dv1[k] for k in image_dv1.keys() & keys},
            'bfu': bfu_dv1['uuid']
        }

        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])
        client.add_files_to_import(files_dv1, import_=import_import1['uuid'])
        client.create_bfu(import_=import_import1['uuid'], keys=files_dv1,
                          **bfu_dv1)
        client.create_image(image_dv1['uuid'], image_dv1['name'],
                            image_dv1['key'], image_dv1['pyramidLevels'],
                            bfu_dv1['uuid'])

        result = client.describe_image(image_dv1['uuid'])

        assert expected == result

    def test_image_nonexistant(self, client, image_dv1):
        with pytest.raises(ValueError):
            client.describe_user(image_dv1['uuid'])

    def test_imports_in_repository(self, client, import_import1,
                                   repository_repo1, user_bob):
        expected = [import_import1]

        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])

        result = client.list_imports_in_repository(repository_repo1['uuid'])
        print(expected)
        print(result)
        assert_rowsets_equal(expected, result)

    def test_bfus_in_import(self, client, bfu_dv1, files_dv1, import_import1,
                            repository_repo1, user_bob):
        expected = [{**bfu_dv1, **{'entrypoint': files_dv1[0]}}]

        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])
        client.add_files_to_import(files_dv1, import_=import_import1['uuid'])
        client.create_bfu(import_=import_import1['uuid'], keys=files_dv1,
                          **bfu_dv1)

        result = client.list_bfus_in_import(import_import1['uuid'])

        assert_rowsets_equal(expected, result)

    def test_has_user_permission_repository(self, client, repository_repo1,
                                            user_bob):
        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)

        assert client.has_user_permission(user_bob['uuid'],
                                          repository_repo1['uuid'],
                                          permission='Admin')

    def test_hasnt_user_permission_repository(self, client, repository_repo1,
                                              user_bob, user_bill):
        client.create_user(**user_bob)
        client.create_user(**user_bill)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)

        assert False is client.has_user_permission(user_bill['uuid'],
                                                   repository_repo1['uuid'],
                                                   permission='Admin')

    def test_has_user_permission_import(self, client, import_import1,
                                        repository_repo1, user_bob):
        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])

        assert client.has_user_permission(user_bob['uuid'],
                                          import_import1['uuid'],
                                          permission='Admin')

    def test_hasnt_user_permission_import(self, client, import_import1,
                                          repository_repo1, user_bob,
                                          user_bill):
        client.create_user(**user_bob)
        client.create_user(**user_bill)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])

        assert False is client.has_user_permission(user_bill['uuid'],
                                                   import_import1['uuid'],
                                                   permission='Admin')

    def test_has_user_permission_file(self, client, files_dv1, import_import1,
                                      repository_repo1, user_bob):
        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])
        client.add_files_to_import(files_dv1, import_=import_import1['uuid'])

        assert client.has_user_permission(user_bob['uuid'],
                                          files_dv1[0],
                                          permission='Admin',
                                          raw_resource=True)

    def test_hasnt_user_permission_file(self, client, files_dv1,
                                        import_import1, repository_repo1,
                                        user_bob, user_bill):
        client.create_user(**user_bob)
        client.create_user(**user_bill)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])
        client.add_files_to_import(files_dv1, import_=import_import1['uuid'])

        assert False is client.has_user_permission(user_bill['uuid'],
                                                   files_dv1[0],
                                                   permission='Admin',
                                                   raw_resource=True)

    def test_has_user_permission_bfu(self, client, bfu_dv1, files_dv1,
                                     import_import1, repository_repo1,
                                     user_bob):
        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])
        client.add_files_to_import(files_dv1, import_=import_import1['uuid'])
        client.create_bfu(import_=import_import1['uuid'], keys=files_dv1,
                          **bfu_dv1)

        assert client.has_user_permission(user_bob['uuid'],
                                          bfu_dv1['uuid'],
                                          permission='Admin')

    def test_hasnt_user_permission_bfu(self, client, bfu_dv1, files_dv1,
                                       import_import1, repository_repo1,
                                       user_bob, user_bill):
        client.create_user(**user_bob)
        client.create_user(**user_bill)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])
        client.add_files_to_import(files_dv1, import_=import_import1['uuid'])
        client.create_bfu(import_=import_import1['uuid'], keys=files_dv1,
                          **bfu_dv1)

        assert False is client.has_user_permission(user_bill['uuid'],
                                                   bfu_dv1['uuid'],
                                                   permission='Admin')

    def test_has_user_permission_image(self, client, image_dv1, bfu_dv1,
                                       files_dv1, import_import1,
                                       repository_repo1, user_bob):
        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])
        client.add_files_to_import(files_dv1, import_=import_import1['uuid'])
        client.create_bfu(import_=import_import1['uuid'], keys=files_dv1,
                          **bfu_dv1)
        client.create_image(image_dv1['uuid'], image_dv1['name'],
                            image_dv1['key'], image_dv1['pyramidLevels'],
                            bfu_dv1['uuid'])

        assert client.has_user_permission(user_bob['uuid'],
                                          image_dv1['uuid'],
                                          permission='Admin')

    def test_hasnt_user_permission_image(self, client, image_dv1, bfu_dv1,
                                         files_dv1, import_import1,
                                         repository_repo1, user_bob,
                                         user_bill):
        client.create_user(**user_bob)
        client.create_user(**user_bill)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])
        client.add_files_to_import(files_dv1, import_=import_import1['uuid'])
        client.create_bfu(import_=import_import1['uuid'], keys=files_dv1,
                          **bfu_dv1)
        client.create_image(image_dv1['uuid'], image_dv1['name'],
                            image_dv1['key'], image_dv1['pyramidLevels'],
                            bfu_dv1['uuid'])

        assert False is client.has_user_permission(user_bill['uuid'],
                                                   image_dv1['uuid'],
                                                   permission='Admin')

    def test_has_user_permission_implied_read(self, client, repository_repo1,
                                              user_bob):
        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)

        assert client.has_user_permission(user_bob['uuid'],
                                          repository_repo1['uuid'],
                                          permission='Read')

    def test_has_user_permission_implied_write(self, client, repository_repo1,
                                               user_bob):
        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)

        assert client.has_user_permission(user_bob['uuid'],
                                          repository_repo1['uuid'],
                                          permission='Write')

    def test_add_users_to_group(self, client, group_somelab, user_bob,
                                user_bill):
        expected = [user_bob, user_bill]

        client.create_user(**user_bob)
        client.create_user(**user_bill)
        client.create_group(user=user_bob['uuid'], **group_somelab)
        client.add_users_to_group(group_somelab['uuid'], [user_bill['uuid']])

        result = client.list_users_in_group(group_somelab['uuid'])

        assert_rowsets_equal(expected, result)

    def test_is_member(self, client, group_somelab, user_bob):

        client.create_user(**user_bob)
        client.create_group(user=user_bob['uuid'], **group_somelab)

        assert client.is_member(user_bob['uuid'], group_somelab['uuid'])

    def test_isnt_member(self, client, group_somelab, user_bob, user_bill):

        client.create_user(**user_bob)
        client.create_user(**user_bill)
        client.create_group(user=user_bob['uuid'], **group_somelab)

        assert False is client.is_member(user_bill['uuid'],
                                         group_somelab['uuid'])

    def test_import_complete(self, client, import_import1, repository_repo1,
                             user_bob):
        keys = {'name', 'key', 'complete'}
        expected = {
            **{k: import_import1[k] for k in import_import1.keys() & keys},
            'repository': repository_repo1['uuid']
        }
        expected['complete'] = True

        client.create_user(**user_bob)
        client.create_repository(user=user_bob['uuid'], **repository_repo1)
        client.create_import(uuid=import_import1['uuid'],
                             name=import_import1['name'],
                             key=import_import1['key'],
                             repository=repository_repo1['uuid'])

        client.set_import_complete(import_import1['uuid'])

        result = client.describe_import(import_import1['uuid'])
        assert expected == result
