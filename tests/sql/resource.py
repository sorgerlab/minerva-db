import pytest
from sqlalchemy.exc import IntegrityError, DataError
from sqlalchemy.orm.exc import NoResultFound
from src.minerva_db.sql.api import DBError
from src.minerva_db.sql.api.utils import to_jsonapi
from src.minerva_db.sql.models import (Repository, Import, Fileset, Image, Key,
                                       User, Grant, RenderingSettings, Channel)
from .factories import (RepositoryFactory, ImportFactory, FilesetFactory,
                        ImageFactory, KeyFactory)
from . import sa_obj_to_dict, statement_log
import uuid


class TestRepository():

    def test_create_repository(self, client, session, db_user):
        keys = ('uuid', 'name', 'raw_storage')
        d = sa_obj_to_dict(RepositoryFactory(), keys)
        assert to_jsonapi(d) == client.create_repository(
            user_uuid=db_user.uuid,
            **d
        )
        repository = session.query(Repository).one()
        assert d == sa_obj_to_dict(repository, keys)
        assert db_user == repository.subjects[0]

    @pytest.mark.parametrize('duplicate_key', ['uuid', 'name'])
    def test_create_repository_duplicate(self, client, db_user, duplicate_key):
        keys = ('uuid', 'name', 'raw_storage')
        d1 = sa_obj_to_dict(RepositoryFactory(), keys)
        d2 = sa_obj_to_dict(RepositoryFactory(), keys)
        d2[duplicate_key] = d1[duplicate_key]
        client.create_repository(user_uuid=db_user.uuid, **d1)
        with pytest.raises(IntegrityError):
            client.create_repository(user_uuid=db_user.uuid, **d2)

    def test_create_repository_nonexistant_user(self, client, session):
        keys = ('uuid', 'name', 'raw_storage')
        d = sa_obj_to_dict(RepositoryFactory(), keys)
        with pytest.raises(NoResultFound):
            client.create_repository(user_uuid='nonexistant', **d)

    def test_create_repository_nonexistant_raw_storage(self, client, db_user,
                                                       session):
        keys = ('uuid', 'name', 'raw_storage')
        d = sa_obj_to_dict(RepositoryFactory(), keys)
        d['raw_storage'] = 'nonexistant'
        with pytest.raises(DataError):
            client.create_repository(user_uuid=db_user.uuid, **d)

    def test_get_repository(self, client, db_repository):
        keys = ('uuid', 'name', 'raw_storage')
        d = sa_obj_to_dict(db_repository, keys)
        assert to_jsonapi(d) == client.get_repository(db_repository.uuid)

    def test_get_repository_nonexistant(self, client):
        with pytest.raises(NoResultFound):
            client.get_repository('nonexistant')

    def test_get_repository_query_count(self, connection, client,
                                        db_repository):
        repository_uuid = db_repository.uuid
        with statement_log(connection) as statements:
            client.get_repository(repository_uuid)
            assert len(statements) == 1

    def test_update_repository_name(self, client, db_repository):
        keys = ('uuid', 'name', 'raw_storage')
        d = sa_obj_to_dict(db_repository, keys)
        repository = client.update_repository(db_repository.uuid,
                                              name='renamed')
        d['name'] = 'renamed'
        assert to_jsonapi(d) == repository

    def test_update_repository_raw_storage(self, client, db_repository):
        keys = ('uuid', 'name', 'raw_storage')
        d = sa_obj_to_dict(db_repository, keys)
        repository = client.update_repository(db_repository.uuid,
                                              raw_storage='Destroy')
        d['raw_storage'] = 'Destroy'
        assert to_jsonapi(d) == repository

    def test_delete_repository(self, client, session, db_repository):
        client.delete_repository(db_repository.uuid)
        assert 0 == session.query(Repository).count()

    def test_delete_repository_with_contents(self, client, session,
                                             user_granted_read_hierarchy):
        db_repository = user_granted_read_hierarchy['repository']
        client.delete_repository(db_repository.uuid)
        assert 0 == session.query(Repository).count()
        assert 0 == session.query(Import).count()
        assert 0 == session.query(Fileset).count()
        assert 0 == session.query(Image).count()
        assert 0 == session.query(Key).count()
        assert 0 == session.query(Grant).count()
        assert 1 == session.query(User).count()


class TestImport():

    def test_create_import(self, client, session, db_repository):
        keys = ('uuid', 'name')
        import_ = ImportFactory()
        create_d = sa_obj_to_dict(import_, keys)
        keys += ('complete',)
        d = sa_obj_to_dict(import_, keys)
        d['repository_uuid'] = db_repository.uuid
        assert to_jsonapi(d) == client.create_import(
            repository_uuid=db_repository.uuid,
            **create_d
        )
        import_ = session.query(Import).one()
        keys += ('repository_uuid',)
        assert d == sa_obj_to_dict(import_, keys)
        assert db_repository == import_.repository

    @pytest.mark.parametrize('duplicate_key', ['uuid', 'name'])
    def test_create_import_duplicate(self, client, db_repository,
                                     duplicate_key):
        keys = ('uuid', 'name')
        d1 = sa_obj_to_dict(ImportFactory(), keys)
        d2 = sa_obj_to_dict(ImportFactory(), keys)
        d2[duplicate_key] = d1[duplicate_key]
        client.create_import(repository_uuid=db_repository.uuid, **d1)
        with pytest.raises(IntegrityError):
            client.create_import(repository_uuid=db_repository.uuid, **d2)

    def test_create_import_nonexistant_repository(self, client, session):
        keys = ('uuid', 'name')
        d = sa_obj_to_dict(ImportFactory(), keys)
        with pytest.raises(NoResultFound):
            client.create_import(repository_uuid='nonexistant', **d)

    def test_get_import(self, client, db_import):
        keys = ('uuid', 'name', 'complete', 'repository_uuid')
        d = sa_obj_to_dict(db_import, keys)
        assert to_jsonapi(d) == client.get_import(db_import.uuid)

    def test_get_import_nonexistant(self, client):
        with pytest.raises(NoResultFound):
            client.get_import('nonexistant')

    def test_get_import_query_count(self, connection, client, db_import):
        import_uuid = db_import.uuid
        with statement_log(connection) as statements:
            client.get_import(import_uuid)
            assert len(statements) == 1

    def test_list_imports_in_repository(self, client,
                                        user_granted_read_hierarchy):
        keys = ('uuid', 'name', 'complete', 'repository_uuid')
        d = sa_obj_to_dict(user_granted_read_hierarchy['import_'], keys)
        assert to_jsonapi([d]) == client.list_imports_in_repository(
            user_granted_read_hierarchy['repository_uuid']
        )

    def test_list_imports_in_repository_query_count(
        self, connection, client, user_granted_read_hierarchy
    ):
        repository_uuid = user_granted_read_hierarchy['repository_uuid']
        with statement_log(connection) as statements:
            client.list_imports_in_repository(repository_uuid)
            assert len(statements) == 1

    # TODO Test class for Key?
    def test_list_keys_in_import(self, client,
                                 user_granted_read_hierarchy):
        keys = ('key', 'import_uuid', 'fileset_uuid')
        d = sa_obj_to_dict(user_granted_read_hierarchy['key'], keys)
        assert to_jsonapi([d]) == client.list_keys_in_import(
            user_granted_read_hierarchy['import_uuid']
        )

    def test_list_keys_in_import_query_count(self, connection, client,
                                             user_granted_read_hierarchy):
        import_uuid = user_granted_read_hierarchy['import_uuid']
        with statement_log(connection) as statements:
            client.list_keys_in_import(import_uuid)
            assert len(statements) == 1

    def test_update_import_name(self, client, db_import):
        keys = ('uuid', 'name', 'complete', 'repository_uuid')
        d = sa_obj_to_dict(db_import, keys)
        import_ = client.update_import(db_import.uuid,
                                       name='renamed')
        d['name'] = 'renamed'
        assert to_jsonapi(d) == import_

    def test_update_import_complete(self, client, db_import):
        keys = ('uuid', 'name', 'complete', 'repository_uuid')
        d = sa_obj_to_dict(db_import, keys)
        import_ = client.update_import(db_import.uuid,
                                       complete=True)
        d['complete'] = True
        assert to_jsonapi(d) == import_


class TestFileset():

    def test_create_fileset(self, client, session, db_import_with_keys):
        db_keys = [key.key for key in db_import_with_keys.keys[:2]]
        keys = ('uuid', 'name', 'reader', 'reader_software', 'reader_version')
        fileset = FilesetFactory()
        create_d = sa_obj_to_dict(fileset, keys)
        keys += ('complete',)
        d = sa_obj_to_dict(fileset, keys)
        d['import_uuid'] = db_import_with_keys.uuid
        assert to_jsonapi(d) == client.create_fileset(
            import_uuid=db_import_with_keys.uuid,
            keys=db_keys,
            **create_d
        )
        fileset = session.query(Fileset).one()
        keys += ('import_uuid',)
        assert d == sa_obj_to_dict(fileset, keys)
        assert db_import_with_keys == fileset.import_
        assert set(db_keys) == {key.key for key in fileset.keys}

    @pytest.mark.parametrize('duplicate_key', ['uuid'])
    def test_create_fileset_duplicate(self, client, db_import, duplicate_key):
        keys = ('uuid', 'name', 'reader', 'reader_software', 'reader_version')
        d1 = sa_obj_to_dict(FilesetFactory(), keys)
        d2 = sa_obj_to_dict(FilesetFactory(), keys)
        d2[duplicate_key] = d1[duplicate_key]
        client.create_fileset(import_uuid=db_import.uuid, keys=[], **d1)
        with pytest.raises(IntegrityError):
            client.create_fileset(import_uuid=db_import.uuid, keys=[], **d2)

    def test_create_fileset_duplicate_key(self, client, db_import_with_keys):
        db_keys = [db_import_with_keys.keys[0].key]
        keys = ('uuid', 'name', 'reader', 'reader_software', 'reader_version')
        d1 = sa_obj_to_dict(FilesetFactory(), keys)
        d2 = sa_obj_to_dict(FilesetFactory(), keys)
        client.create_fileset(import_uuid=db_import_with_keys.uuid,
                              keys=db_keys, **d1)
        with pytest.raises(DBError):
            client.create_fileset(import_uuid=db_import_with_keys.uuid,
                                  keys=db_keys, **d2)

    def test_create_fileset_nonexistant_import(self, client, session):
        keys = ('uuid', 'name', 'reader', 'reader_software', 'reader_version')
        d = sa_obj_to_dict(FilesetFactory(), keys)
        with pytest.raises(NoResultFound):
            client.create_fileset(import_uuid='nonexistant', keys=[], **d)

    def test_create_fileset_with_duplicate_key_in_different_imports(self,
                                                                    client,
                                                                    session):
        keys = ('uuid', 'name', 'reader', 'reader_software', 'reader_version')
        import1 = ImportFactory()
        import2 = ImportFactory()
        key1 = KeyFactory(import_=import1, key='key')
        key2 = KeyFactory(import_=import2, key='key')
        session.add_all([import1, import2, key1, key2])
        session.commit()

        fileset1 = FilesetFactory()
        fileset2 = FilesetFactory()
        d1 = sa_obj_to_dict(fileset1, keys)
        d2 = sa_obj_to_dict(fileset2, keys)
        client.create_fileset(import_uuid=import1.uuid, keys=['key'], **d1)
        client.create_fileset(import_uuid=import2.uuid, keys=['key'], **d2)

    def test_get_fileset(self, client, db_fileset):
        keys = ('uuid', 'name', 'reader', 'reader_software', 'reader_version',
                'complete', 'import_uuid')
        d = sa_obj_to_dict(db_fileset, keys)
        assert to_jsonapi(d) == client.get_fileset(db_fileset.uuid)

    def test_get_fileset_nonexistant(self, client):
        with pytest.raises(NoResultFound):
            client.get_fileset('nonexistant')

    def test_get_fileset_query_count(self, connection, client, db_fileset):
        fileset_uuid = db_fileset.uuid
        with statement_log(connection) as statements:
            client.get_fileset(fileset_uuid)
            assert len(statements) == 1

    def test_list_filesets_in_import(self, client,
                                     user_granted_read_hierarchy):
        keys = ('uuid', 'name', 'reader', 'reader_software', 'reader_version',
                'complete', 'import_uuid')
        d = sa_obj_to_dict(user_granted_read_hierarchy['fileset'], keys)
        assert to_jsonapi([d]) == client.list_filesets_in_import(
            user_granted_read_hierarchy['import_uuid']
        )

    def test_list_filesets_in_import_query_count(self, connection, client,
                                                 user_granted_read_hierarchy):
        import_uuid = user_granted_read_hierarchy['import_uuid']
        with statement_log(connection) as statements:
            client.list_filesets_in_import(import_uuid)
            assert len(statements) == 1

    # TODO Test class for Key?
    def test_list_keys_in_fileset(self, client,
                                  user_granted_read_hierarchy):
        keys = ('key', 'import_uuid', 'fileset_uuid')
        d = sa_obj_to_dict(user_granted_read_hierarchy['key'], keys)
        assert to_jsonapi([d]) == client.list_keys_in_fileset(
            user_granted_read_hierarchy['fileset_uuid']
        )

    def test_list_keys_in_fileset_query_count(self, connection, client,
                                              user_granted_read_hierarchy):
        fileset_uuid = user_granted_read_hierarchy['fileset_uuid']
        with statement_log(connection) as statements:
            client.list_keys_in_fileset(fileset_uuid)
            assert len(statements) == 1

    def test_update_fileset_complete(self, client, db_fileset):
        keys = ('uuid', 'name', 'reader', 'reader_software', 'reader_version',
                'complete', 'import_uuid')
        d = sa_obj_to_dict(db_fileset, keys)
        d['complete'] = True
        assert to_jsonapi(d) == client.update_fileset(db_fileset.uuid,
                                                      complete=True)

    def test_update_fileset_complete_with_images(self, client, db_fileset):
        keys = ('uuid', 'name', 'reader', 'reader_software', 'reader_version',
                'complete', 'import_uuid')
        d = sa_obj_to_dict(db_fileset, keys)
        d_image = sa_obj_to_dict(ImageFactory(), ['uuid', 'name',
                                                  'pyramid_levels'])
        d['complete'] = True
        assert to_jsonapi(d) == client.update_fileset(db_fileset.uuid,
                                                      complete=True,
                                                      images=[d_image])
        image = client.list_images_in_fileset(db_fileset.uuid)
        d_image['fileset_uuid'] = db_fileset.uuid
        assert to_jsonapi([d_image]) == image

    def test_update_fileset_incomplete_with_images(self, client, db_fileset):
        d_image = sa_obj_to_dict(ImageFactory(), ['uuid', 'name',
                                                  'pyramid_levels'])
        with pytest.raises(DBError):
            client.update_fileset(db_fileset.uuid, images=[d_image])


class TestImage():

    def test_create_image(self, client, session, db_fileset):
        keys = ('uuid', 'name', 'pyramid_levels')
        image = ImageFactory()
        create_d = sa_obj_to_dict(image, keys)
        d = sa_obj_to_dict(image, keys)
        d['fileset_uuid'] = db_fileset.uuid
        assert to_jsonapi(d) == client.create_image(
            fileset_uuid=db_fileset.uuid,
            **create_d
        )
        image = session.query(Image).one()
        keys += ('fileset_uuid',)
        assert d == sa_obj_to_dict(image, keys)
        assert db_fileset == image.fileset

    @pytest.mark.parametrize('duplicate_key', ['uuid'])
    def test_create_image_duplicate(self, client, db_fileset, duplicate_key):
        keys = ('uuid', 'name', 'pyramid_levels')
        d1 = sa_obj_to_dict(ImageFactory(), keys)
        d2 = sa_obj_to_dict(ImageFactory(), keys)
        d2[duplicate_key] = d1[duplicate_key]
        client.create_image(fileset_uuid=db_fileset.uuid, **d1)
        with pytest.raises(IntegrityError):
            client.create_image(fileset_uuid=db_fileset.uuid, **d2)

    def test_create_image_nonexistant_repository(self, client, session):
        keys = ('uuid', 'name', 'pyramid_levels')
        d = sa_obj_to_dict(ImageFactory(), keys)
        with pytest.raises(NoResultFound):
            client.create_image(fileset_uuid='nonexistant', **d)

    def test_get_image(self, client, db_image):
        keys = ('uuid', 'name', 'pyramid_levels', 'fileset_uuid')
        d = sa_obj_to_dict(db_image, keys)
        assert to_jsonapi(d) == client.get_image(db_image.uuid)

    def test_get_image_nonexistant(self, client):
        with pytest.raises(NoResultFound):
            client.get_image('nonexistant')

    def test_get_image_query_count(self, connection, client, db_image):
        image_uuid = db_image.uuid
        with statement_log(connection) as statements:
            client.get_image(image_uuid)
            assert len(statements) == 1

    def test_list_images_in_fileset(self, client,
                                    user_granted_read_hierarchy):
        keys = ('uuid', 'name', 'pyramid_levels', 'fileset_uuid')
        d = sa_obj_to_dict(user_granted_read_hierarchy['image'], keys)
        assert to_jsonapi([d]) == client.list_images_in_fileset(
            user_granted_read_hierarchy['fileset_uuid']
        )

    def test_list_images_in_fileset_query_count(self, connection, client,
                                                user_granted_read_hierarchy):
        fileset_uuid = user_granted_read_hierarchy['fileset_uuid']
        with statement_log(connection) as statements:
            client.list_images_in_fileset(fileset_uuid)
            assert len(statements) == 1


class TestRenderingSettings:

    def test_create_rendering_settings(self, client, session, db_image):
        channel1 = Channel("1", "DNA", "0000FF", 0.2, 0.5)
        channel2 = Channel("2", "CD4", "00FF00", 0, 1)
        channels = [channel1.as_dict(), channel2.as_dict()]
        client.create_rendering_settings(uuid.uuid4(), db_image.uuid, channels, "Macrophages")

        res = session.query(RenderingSettings).filter(RenderingSettings.image_uuid == db_image.uuid).all()
        assert len(res) == 1
        assert res[0].channels[0]["color"] == "0000FF"
        assert res[0].channels[0]["min"] == 0.2
        assert res[0].channels[0]["max"] == 0.5

        assert res[0].channels[1]["color"] == "00FF00"
        assert res[0].channels[1]["min"] == 0
        assert res[0].channels[1]["max"] == 1

    def test_update_rendering_settings(self, client, session, db_image):
        channel1 = Channel("1", "DNA", "0000FF", 0.2, 0.5)
        channel2 = Channel("2", "CD4", "00FF00", 0, 1)
        channels = [channel1.as_dict(), channel2.as_dict()]
        cg_uuid = uuid.uuid4()
        client.create_rendering_settings(cg_uuid, db_image.uuid, channels, "Macrophages")

        channels.append(Channel("3", "NEW", "FFFFFF", 0.3, 0.7).as_dict())
        channels[0]["min"] = 0.15
        channels[0]["max"] = 0.99
        client.update_rendering_settings(cg_uuid, channels, "Macrophages")

        res = session.query(RenderingSettings).filter(RenderingSettings.image_uuid == db_image.uuid).all()
        assert len(res[0].channels) == 3
        assert res[0].channels[0]["min"] == 0.15
        assert res[0].channels[0]["max"] == 0.99


    def test_get_rendering_settings(self, client, db_image):
        channels1 = [
            Channel("1", "DNA", "0000FF", 0.2, 0.5).as_dict(),
            Channel("2", "CD163", "FF0000", 0, 1).as_dict(),
            Channel("3", "IBA1", "00FF00", 0.5, 0.8).as_dict()
        ]

        client.create_rendering_settings(uuid.uuid4(), db_image.uuid, channels1, "Macrophages")

        channels2 = [
            Channel("1", "CD3D", "0000FF", 0.2, 0.5).as_dict(),
            Channel("2", "CD20", "00FF00", 0, 1).as_dict()
        ]

        client.create_rendering_settings(uuid.uuid4(), db_image.uuid, channels2, "Lymphocytes")

        rendering_settings = client.list_rendering_settings(db_image.uuid)
        assert len(rendering_settings) == 2
        rendering_settings = sorted(rendering_settings, key=lambda x: x.label)
        assert rendering_settings[0].label == "Lymphocytes"
        assert len(rendering_settings[0].channels) == 2
        assert rendering_settings[0].channels[1]["label"] == "CD20"
        assert rendering_settings[0].channels[1]["color"] == "00FF00"
        assert rendering_settings[0].channels[1]["min"] == 0
        assert rendering_settings[0].channels[1]["max"] == 1

        assert rendering_settings[1].label == "Macrophages"
        assert len(rendering_settings[1].channels) == 3

    def test_get_channel_group(self, client, db_image):
        channels = [
            Channel("1", "DNA", "0000FF", 0.2, 0.5).as_dict(),
            Channel("2", "CD4", "FF0000", 0, 1).as_dict()
        ]

        cg_uuid = uuid.uuid4()
        client.create_rendering_settings(cg_uuid, db_image.uuid, channels, "1")

        channel_group = client.get_image_channel_group(str(cg_uuid))
        assert len(channel_group.channels) == 2







