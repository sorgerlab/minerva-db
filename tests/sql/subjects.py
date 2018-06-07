import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from minerva_db.sql.models import User, Group, Membership
from .factories import GroupFactory, UserFactory, MembershipFactory
from .utils import sa_obj_to_dict


class TestUser():

    def test_create_user(self, client, session):
        keys = ('uuid', 'name', 'email')
        d = sa_obj_to_dict(UserFactory(), keys)
        assert d == client.create_user(**d)
        assert d == sa_obj_to_dict(session.query(User).one(), keys)

    @pytest.mark.parametrize('duplicate_key', ['uuid', 'name', 'email'])
    def test_create_user_duplicate(self, client, duplicate_key):
        keys = ('uuid', 'name', 'email')
        d1 = sa_obj_to_dict(UserFactory(), keys)
        d2 = sa_obj_to_dict(UserFactory(), keys)
        d2[duplicate_key] = d1[duplicate_key]
        client.create_user(**d1)
        with pytest.raises(IntegrityError):
            client.create_user(**d2)

    def test_get_user(self, client, db_user):
        keys = ('uuid', 'name', 'email')
        d = sa_obj_to_dict(db_user, keys)
        assert client.get_user(db_user.uuid) == d

    def test_get_user_nonexistant(self, client):
        with pytest.raises(NoResultFound):
            client.get_user('nonexistant')


class TestGroup():

    def test_create_group(self, client, session, db_user):
        keys = ('uuid', 'name')
        d = sa_obj_to_dict(GroupFactory(), keys)
        assert d == client.create_group(user_uuid=db_user.uuid, **d)
        assert d == sa_obj_to_dict(session.query(Group).one(), keys)

    @pytest.mark.parametrize('duplicate_key', ['uuid', 'name'])
    def test_create_group_duplicate(self, client, db_user, duplicate_key):
        keys = ('uuid', 'name')
        d1 = sa_obj_to_dict(GroupFactory(), keys)
        d2 = sa_obj_to_dict(GroupFactory(), keys)
        d2[duplicate_key] = d1[duplicate_key]
        client.create_group(user_uuid=db_user.uuid, **d1)
        with pytest.raises(IntegrityError):
            client.create_group(user_uuid=db_user.uuid, **d2)

    def test_get_group(self, client, db_group):
        keys = ('uuid', 'name')
        d = sa_obj_to_dict(db_group, keys)
        assert client.get_group(db_group.uuid) == d

    def test_get_group_nonexistant(self, client):
        with pytest.raises(NoResultFound):
            client.get_group('nonexistant')


class TestMembership():

    def test_create_membership(self, client, session, db_user, db_group):
        keys = ['user_uuid', 'group_uuid', 'membership_type']
        d = sa_obj_to_dict(MembershipFactory(), keys)
        d['user_uuid'] = db_user.uuid
        d['group_uuid'] = db_group.uuid
        m = client.create_membership(db_user.uuid, db_group.uuid, 'Member')
        assert d == m
        assert d == sa_obj_to_dict(session.query(Membership).one(), keys)

    def test_create_membership_duplicate(self, client, session, db_user,
                                         db_group):
        client.create_membership(db_user.uuid, db_group.uuid, 'Member')
        with pytest.raises(IntegrityError):
            client.create_membership(db_user.uuid, db_group.uuid, 'Owner')

    def test_create_membership_nonexistant_user(self, client, session,
                                                db_group):
        with pytest.raises(NoResultFound):
            client.create_membership('nonexistant', db_group.uuid, 'Member')

    def test_create_membership_nonexistant_group(self, client, session,
                                                 db_user):
        with pytest.raises(NoResultFound):
            client.create_membership(db_user.uuid, 'nonexistant', 'Member')

    def test_add_user_to_group(self, client, session, db_user, db_group):
        client.add_users_to_group(db_group.uuid, [db_user.uuid])
        assert db_user == session.query(Group).one().users[0]
        assert db_group == session.query(User).one().groups[0]

    def test_add_users_to_nonexistant_group(self, client, session, db_user):
        with pytest.raises(NoResultFound):
            client.add_users_to_group('nonexistant', [db_user.uuid])

    def test_add_nonexistant_user_to_group(self, client, session, db_group):
        with pytest.raises(NoResultFound):
            client.add_users_to_group(db_group.uuid, ['nonexistant'])

    # TODO Add user to group when already a member
    # TODO Remove user from group when not a member

    def test_is_member(self, client, db_membership):
        assert client.is_member(db_membership.user_uuid,
                                db_membership.group_uuid)

    def test_isnt_member(self, client, db_user, db_group):
        assert not client.is_member(db_user.uuid,
                                    db_group.uuid)

    def test_is_owner(self, client, db_ownership):
        assert client.is_owner(db_ownership.user_uuid,
                               db_ownership.group_uuid)

    def test_isnt_owner(self, client, db_membership):
        assert not client.is_owner(db_membership.user_uuid,
                                   db_membership.group_uuid)
