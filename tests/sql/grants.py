import pytest
from minerva_db.sql.api.utils import to_jsonapi
from .utils import sa_obj_to_dict


@pytest.mark.parametrize('fixture_name', ['user_granted_read_hierarchy',
                                          'group_granted_read_hierarchy'])
class TestGrants():

    def test_repository(self, client, fixture_name, request):
        hierarchy = request.getfuncargvalue(fixture_name)
        user_uuid = hierarchy['user'].uuid
        repository_uuid = hierarchy['repository'].uuid
        decision = client.has_permission(user_uuid, 'Repository',
                                         repository_uuid, 'Read')
        assert True is decision

    def test_repository_insufficent(self, client, fixture_name, request):
        hierarchy = request.getfuncargvalue(fixture_name)
        user_uuid = hierarchy['user'].uuid
        repository_uuid = hierarchy['repository'].uuid
        decision = client.has_permission(user_uuid, 'Repository',
                                         repository_uuid, 'Write')
        assert False is decision

    def test_repository_none(self, client, db_user, fixture_name, request):
        hierarchy = request.getfuncargvalue(fixture_name)
        user_uuid = db_user.uuid
        repository_uuid = hierarchy['repository'].uuid
        decision = client.has_permission(user_uuid, 'Repository',
                                         repository_uuid, 'Read')
        assert False is decision

    def test_import(self, client, fixture_name, request):
        hierarchy = request.getfuncargvalue(fixture_name)
        user_uuid = hierarchy['user'].uuid
        import_uuid = hierarchy['import_'].uuid
        decision = client.has_permission(user_uuid, 'Import', import_uuid,
                                         'Read')
        assert True is decision

    def test_import_insufficent(self, client, fixture_name, request):
        hierarchy = request.getfuncargvalue(fixture_name)
        user_uuid = hierarchy['user'].uuid
        import_uuid = hierarchy['import_'].uuid
        decision = client.has_permission(user_uuid, 'Import', import_uuid,
                                         'Write')
        assert False is decision

    def test_import_none(self, client, db_user, fixture_name, request):
        hierarchy = request.getfuncargvalue(fixture_name)
        user_uuid = db_user.uuid
        import_uuid = hierarchy['import_'].uuid
        decision = client.has_permission(user_uuid, 'Import', import_uuid,
                                         'Read')
        assert False is decision

    def test_bfu(self, client, fixture_name, request):
        hierarchy = request.getfuncargvalue(fixture_name)
        user_uuid = hierarchy['user'].uuid
        bfu_uuid = hierarchy['bfu'].uuid
        decision = client.has_permission(user_uuid, 'BFU', bfu_uuid, 'Read')
        assert True is decision

    def test_bfu_insufficent(self, client, fixture_name, request):
        hierarchy = request.getfuncargvalue(fixture_name)
        user_uuid = hierarchy['user'].uuid
        bfu_uuid = hierarchy['bfu'].uuid
        decision = client.has_permission(user_uuid, 'BFU', bfu_uuid, 'Write')
        assert False is decision

    def test_bfu_none(self, client, db_user, fixture_name, request):
        hierarchy = request.getfuncargvalue(fixture_name)
        user_uuid = db_user.uuid
        bfu_uuid = hierarchy['bfu'].uuid
        decision = client.has_permission(user_uuid, 'BFU', bfu_uuid, 'Read')
        assert False is decision

    def test_image(self, client, fixture_name, request):
        hierarchy = request.getfuncargvalue(fixture_name)
        user_uuid = hierarchy['user'].uuid
        image_uuid = hierarchy['image'].uuid
        decision = client.has_permission(user_uuid, 'Image', image_uuid,
                                         'Read')
        assert True is decision

    def test_image_insufficent(self, client, fixture_name, request):
        hierarchy = request.getfuncargvalue(fixture_name)
        user_uuid = hierarchy['user'].uuid
        image_uuid = hierarchy['image'].uuid
        decision = client.has_permission(user_uuid, 'Image', image_uuid,
                                         'Write')
        assert False is decision

    def test_image_none(self, client, db_user, fixture_name, request):
        hierarchy = request.getfuncargvalue(fixture_name)
        user_uuid = db_user.uuid
        image_uuid = hierarchy['image'].uuid
        decision = client.has_permission(user_uuid, 'Image', image_uuid,
                                         'Read')
        assert False is decision


class TestLists():

    def test_list_repositories_for_user(self, client,
                                        user_granted_read_hierarchy):
        grant_keys = ['subject_uuid', 'repository_uuid', 'permission']
        repository_keys = ['uuid', 'name', 'raw_storage']
        user_uuid = user_granted_read_hierarchy['user_uuid']
        d_grant = sa_obj_to_dict(
            user_granted_read_hierarchy['grant'],
            grant_keys
        )
        d_repository = sa_obj_to_dict(
            user_granted_read_hierarchy['repository'],
            repository_keys
        )

        assert to_jsonapi(
            [d_grant], {
                'repositories': [d_repository]
            }
        ) == client.list_repositories_for_user(user_uuid)

    @pytest.mark.parametrize('fixture_name', ['user_granted_read_hierarchy',
                                              'group_granted_read_hierarchy'])
    def test_list_repositories_for_user_implied(self, client, fixture_name,
                                                request):
        hierarchy = request.getfuncargvalue(fixture_name)
        grant_keys = ['subject_uuid', 'repository_uuid', 'permission']
        repository_keys = ['uuid', 'name', 'raw_storage']
        user_uuid = hierarchy['user_uuid']
        d_grant = sa_obj_to_dict(
            hierarchy['grant'],
            grant_keys
        )
        d_repository = sa_obj_to_dict(
            hierarchy['repository'],
            repository_keys
        )

        assert to_jsonapi(
            [d_grant], {
                'repositories': [d_repository]
            }
        ) == client.list_repositories_for_user(user_uuid, implied=True)

    def test_list_repositories_for_user_none(self, client, db_user):
        assert to_jsonapi(
            [], {
                'repositories': []
            }
        ) == client.list_repositories_for_user(db_user.uuid, implied=True)
