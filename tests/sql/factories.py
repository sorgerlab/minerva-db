from factory import Factory, SubFactory, LazyAttribute, Sequence
from minerva_db.sql.models import (User, Group, Repository, Import, Key, BFU,
                                   Image, Grant, Membership)


class GroupFactory(Factory):

    class Meta:
        model = Group

    uuid = LazyAttribute(lambda o: o.name.lower().replace(' ', '_'))
    name = Sequence(lambda n: f'group{n}')


class UserFactory(Factory):

    class Meta:
        model = User

    uuid = Sequence(lambda n: f'user{n}')


class MembershipFactory(Factory):

    class Meta:
        model = Membership

    group = SubFactory(GroupFactory)
    user = SubFactory(UserFactory)
    membership_type = 'Member'


class MembershipOwnerFactory(MembershipFactory):

    membership_type = 'Owner'


class RepositoryFactory(Factory):

    class Meta:
        model = Repository

    uuid = LazyAttribute(lambda o: o.name.lower().replace(' ', '_'))
    name = Sequence(lambda n: f'repository{n}')


class GrantFactory(Factory):

    class Meta:
        model = Grant

    subject = SubFactory(UserFactory)
    repository = SubFactory(RepositoryFactory)
    permission = 'Read'


class GrantAdminFactory(GrantFactory):

    permission = 'Admin'


class ImportFactory(Factory):

    class Meta:
        model = Import

    uuid = LazyAttribute(lambda o: o.name.lower().replace(' ', '_'))
    name = Sequence(lambda n: f'import{n}')
    repository = SubFactory(RepositoryFactory)


class KeyFactory(Factory):

    class Meta:
        model = Key

    key = Sequence(lambda n: f'key{n}')
    import_ = SubFactory(ImportFactory)


class BFUFactory(Factory):

    class Meta:
        model = BFU

    uuid = LazyAttribute(lambda o: o.name.lower().replace(' ', '_'))
    name = Sequence(lambda n: f'bfu{n}')
    reader = 'reader'
    reader_software = 'BioFormats'
    reader_version = '1.0.0'
    complete = False
    import_ = SubFactory(ImportFactory)


class KeyBFUFactory(KeyFactory):

    bfu = SubFactory(BFUFactory)


class ImageFactory(Factory):

    class Meta:
        model = Image

    uuid = LazyAttribute(lambda o: o.name.lower().replace(' ', '_'))
    name = Sequence(lambda n: f'image{n}')
    pyramid_levels = 1
    bfu = SubFactory(BFUFactory)
