import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from minerva_db.sql.api import DBError
from minerva_db.sql.models import Repository, Import, BFU, Image
from .factories import (RepositoryFactory, ImportFactory, BFUFactory,
                        ImageFactory)
from .utils import sa_obj_to_dict


class TestRepository():

    def test_create_repository(self, client, session, db_user):
        keys = ('uuid', 'name')
        d = sa_obj_to_dict(RepositoryFactory(), keys)
        assert d == client.create_repository(user_uuid=db_user.uuid, **d)
        repository = session.query(Repository).one()
        assert d == sa_obj_to_dict(repository, keys)
        assert db_user == repository.subjects[0]

    @pytest.mark.parametrize('duplicate_key', ['uuid', 'name'])
    def test_create_repository_duplicate(self, client, db_user, duplicate_key):
        keys = ('uuid', 'name')
        d1 = sa_obj_to_dict(RepositoryFactory(), keys)
        d2 = sa_obj_to_dict(RepositoryFactory(), keys)
        d2[duplicate_key] = d1[duplicate_key]
        client.create_repository(user_uuid=db_user.uuid, **d1)
        with pytest.raises(IntegrityError):
            client.create_repository(user_uuid=db_user.uuid, **d2)

    def test_create_repository_nonexistant_user(self, client, session):
        keys = ('uuid', 'name')
        d = sa_obj_to_dict(RepositoryFactory(), keys)
        with pytest.raises(NoResultFound):
            client.create_repository(user_uuid='nonexistant', **d)

    def test_get_repository(self, client, db_repository):
        keys = ('uuid', 'name')
        d = sa_obj_to_dict(db_repository, keys)
        assert client.get_repository(db_repository.uuid) == d

    def test_get_repository_nonexistant(self, client):
        with pytest.raises(NoResultFound):
            client.get_repository('nonexistant')


class TestImport():

    def test_create_import(self, client, session, db_repository):
        keys = ('uuid', 'name')
        import_ = ImportFactory()
        create_d = sa_obj_to_dict(import_, keys)
        keys = keys + ('complete',)
        d = sa_obj_to_dict(import_, keys)
        assert d == client.create_import(repository_uuid=db_repository.uuid,
                                         **create_d)
        import_ = session.query(Import).one()
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
        keys = ('uuid', 'name', 'complete')
        d = sa_obj_to_dict(db_import, keys)
        assert d == client.get_import(db_import.uuid)

    def test_get_import_nonexistant(self, client):
        with pytest.raises(NoResultFound):
            client.get_import('nonexistant')

    def test_list_imports_in_repository(self, client,
                                        user_granted_read_hierarchy):
        keys = ('uuid', 'name', 'complete')
        d = sa_obj_to_dict(user_granted_read_hierarchy['import_'], keys)
        assert [d] == client.list_imports_in_repository(
            user_granted_read_hierarchy['repository_uuid']
        )

    # TODO Test class for Key?
    def test_list_keys_in_import(self, client,
                                 user_granted_read_hierarchy):
        keys = ('key',)
        d = sa_obj_to_dict(user_granted_read_hierarchy['key'], keys)
        assert [d] == client.list_keys_in_import(
            user_granted_read_hierarchy['import_uuid']
        )

    def test_set_import_complete(self, client, db_import):
        keys = ('uuid', 'name', 'complete')
        d = sa_obj_to_dict(db_import, keys)
        d['complete'] = True
        assert d == client.set_import_complete(db_import.uuid)


class TestBFU():

    def test_create_bfu(self, client, session, db_import_with_keys):
        db_keys = [key.key for key in db_import_with_keys.keys[:2]]
        keys = ('uuid', 'name', 'reader')
        bfu = BFUFactory()
        create_d = sa_obj_to_dict(bfu, keys)
        keys = keys + ('complete',)
        d = sa_obj_to_dict(bfu, keys)
        assert d == client.create_bfu(import_uuid=db_import_with_keys.uuid,
                                      keys=db_keys,
                                      **create_d)
        bfu = session.query(BFU).one()
        assert d == sa_obj_to_dict(bfu, keys)
        assert db_import_with_keys == bfu.import_
        assert set(db_keys) == {key.key for key in bfu.keys}

    @pytest.mark.parametrize('duplicate_key', ['uuid', 'name'])
    def test_create_bfu_duplicate(self, client, db_import,
                                  duplicate_key):
        keys = ('uuid', 'name', 'reader')
        d1 = sa_obj_to_dict(BFUFactory(), keys)
        d2 = sa_obj_to_dict(BFUFactory(), keys)
        d2[duplicate_key] = d1[duplicate_key]
        client.create_bfu(import_uuid=db_import.uuid, keys=[],
                          **d1)
        with pytest.raises(IntegrityError):
            client.create_bfu(import_uuid=db_import.uuid,
                              keys=[], **d2)

    def test_create_bfu_duplicate_key(self, client, db_import_with_keys):
        db_keys = [db_import_with_keys.keys[0].key]
        keys = ('uuid', 'name', 'reader')
        d1 = sa_obj_to_dict(BFUFactory(), keys)
        d2 = sa_obj_to_dict(BFUFactory(), keys)
        client.create_bfu(import_uuid=db_import_with_keys.uuid, keys=db_keys,
                          **d1)
        with pytest.raises(DBError):
            client.create_bfu(import_uuid=db_import_with_keys.uuid,
                              keys=db_keys, **d2)

    def test_create_bfu_nonexistant_import(self, client, session):
        keys = ('uuid', 'name', 'reader')
        d = sa_obj_to_dict(BFUFactory(), keys)
        with pytest.raises(NoResultFound):
            client.create_bfu(import_uuid='nonexistant', keys=[], **d)

    def test_get_bfu(self, client, db_bfu):
        keys = ('uuid', 'name', 'reader', 'complete')
        d = sa_obj_to_dict(db_bfu, keys)
        assert client.get_bfu(db_bfu.uuid) == d

    def test_get_bfu_nonexistant(self, client):
        with pytest.raises(NoResultFound):
            client.get_bfu('nonexistant')

    def test_list_bfus_in_import(self, client,
                                 user_granted_read_hierarchy):
        keys = ('uuid', 'name', 'reader', 'complete')
        d = sa_obj_to_dict(user_granted_read_hierarchy['bfu'], keys)
        assert [d] == client.list_bfus_in_import(
            user_granted_read_hierarchy['import_uuid']
        )

    # TODO Test class for Key?
    def test_list_keys_in_bfu(self, client,
                              user_granted_read_hierarchy):
        keys = ('key',)
        d = sa_obj_to_dict(user_granted_read_hierarchy['key'], keys)
        assert [d] == client.list_keys_in_bfu(
            user_granted_read_hierarchy['bfu_uuid']
        )

class TestImage():

    def test_create_image(self, client, session, db_bfu):
        keys = ('uuid', 'name', 'key', 'pyramid_levels')
        image = ImageFactory()
        d = sa_obj_to_dict(image, keys)
        assert d == client.create_image(bfu_uuid=db_bfu.uuid,
                                        **d)
        image = session.query(Image).one()
        assert d == sa_obj_to_dict(image, keys)
        assert db_bfu == image.bfu

    @pytest.mark.parametrize('duplicate_key', ['uuid'])
    def test_create_image_duplicate(self, client, db_bfu, duplicate_key):
        keys = ('uuid', 'name', 'key', 'pyramid_levels')
        d1 = sa_obj_to_dict(ImageFactory(), keys)
        d2 = sa_obj_to_dict(ImageFactory(), keys)
        d2[duplicate_key] = d1[duplicate_key]
        client.create_image(bfu_uuid=db_bfu.uuid, **d1)
        with pytest.raises(IntegrityError):
            client.create_image(bfu_uuid=db_bfu.uuid, **d2)

    def test_create_image_nonexistant_repository(self, client, session):
        keys = ('uuid', 'name', 'key', 'pyramid_levels')
        d = sa_obj_to_dict(ImageFactory(), keys)
        with pytest.raises(NoResultFound):
            client.create_image(bfu_uuid='nonexistant', **d)

    def test_get_image(self, client, db_image):
        keys = ('uuid', 'name', 'key', 'pyramid_levels')
        d = sa_obj_to_dict(db_image, keys)
        assert client.get_image(db_image.uuid) == d

    def test_get_image_nonexistant(self, client):
        with pytest.raises(NoResultFound):
            client.get_image('nonexistant')

    def test_list_images_in_bfu(self, client,
                                user_granted_read_hierarchy):
        keys = ('uuid', 'name', 'key', 'pyramid_levels')
        d = sa_obj_to_dict(user_granted_read_hierarchy['image'], keys)
        assert [d] == client.list_images_in_bfu(
            user_granted_read_hierarchy['bfu_uuid']
        )
