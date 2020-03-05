from sqlalchemy.orm import Session, joinedload
from typing import Dict, List, Optional, Union
from ..models import (User, Group, Membership, Repository, Import,
                      Fileset, Image, Key, Grant, RenderingSettings)
from ..serializers import (user_schema, group_schema, repository_schema,
                           repositories_schema, import_schema, imports_schema,
                           keys_schema, fileset_schema, filesets_schema,
                           image_schema, images_schema, grants_schema,
                           membership_schema, rendering_settings_schema)
from . import premade
from .utils import to_jsonapi


class DBError(Exception):
    pass


SDict = Dict[str, Union[str, float, int]]


class Client():

    def __init__(self, session: Session):
        self.session = session

    def _session(self) -> Session:
        '''Get session.

        Returns:
            The client's session.
        '''

        return self.session

    def create_group(self, uuid: str, name: str, user_uuid: str) -> SDict:
        '''Create a group with the specified user as a member.

        Args:
            uuid: UUID of the group.
            name: Name of the group.
            user_uuid: UUID of the user to be the initial member.

        Returns:
            The newly created group.
        '''

        user = self.session.query(User).filter(User.uuid == user_uuid).one()
        group = Group(uuid, name)
        membership = Membership(group, user, 'Owner')
        self.session.add(group)
        self.session.add(membership)
        self.session.commit()
        return to_jsonapi(group_schema.dump(group))

    def create_user(self, uuid: str, name: str=None) -> SDict:
        '''Create a user.

        Args:
            uuid: UUID of the user.

        Returns:
            The newly created user.
        '''

        user = User(uuid, name)
        self.session.add(user)
        self.session.commit()
        return to_jsonapi(user_schema.dump(user))

    def create_membership(self, group_uuid: str, user_uuid: str,
                          membership_type: str) -> SDict:
        '''Create a group membership with the specified user as the member.

        Args:
            group_uuid: UUID of the group.
            user_uuid: UUID of the user.
            membership_type: Membership level the member shall be given.

        Returns:
            The newly created membership.
        '''

        group = (
            self.session.query(Group)
            .filter(Group.uuid == group_uuid)
            .one()
        )

        user = (
            self.session.query(User)
            .filter(User.uuid == user_uuid)
            .one()
        )

        membership = Membership(group, user, membership_type)
        self.session.add(membership)
        self.session.commit()
        return to_jsonapi(membership_schema.dump(membership))

    # Resources
    def create_repository(self, uuid: str, name: str, user_uuid: str,
                          raw_storage: Optional[str] = None) -> SDict:
        '''Create a repository with the specified user as an admin.

        Args:
            uuid: UUID of the repository.
            name: Name of the repository.
            user_uuid: UUID of the user to be initial admin.
            raw_storage: Storage level of the raw data. Default: `None`
                indicates 'Archive'.

        Returns:
            The newly created repository.
        '''

        user = self.session.query(User).filter(User.uuid == user_uuid).one()
        repository = Repository(uuid, name, raw_storage)
        grant = Grant(user, repository, permission='Admin')
        self.session.add_all((repository, grant))
        self.session.commit()
        return to_jsonapi(repository_schema.dump(repository))

    def create_import(self, uuid: str, name: str,
                      repository_uuid: str) -> SDict:
        '''Create an import within the specified repository.

        Args:
            uuid: UUID of the import.
            name: Name of the import.
            key: Prefix key of the import.
            repository_uuid: UUID of the repository.

        Returns:
            The newly created import.
        '''

        repository = self.session.query(Repository) \
            .filter(Repository.uuid == repository_uuid) \
            .one()
        import_ = Import(uuid, name, repository)
        self.session.add(import_)
        self.session.commit()
        return to_jsonapi(import_schema.dump(import_))

    def create_fileset(self, uuid: str, name: str, reader: str,
                       reader_software: str, reader_version: str,
                       keys: List[str], import_uuid: str, progress=0) -> SDict:
        '''Create a fileset within the specified import.

        Associates the given files.

        Args:
            uuid: UUID of the Fileset.
            name: Name of the Fileset.
            reader: Specific reader used to read the Fileset.
            reader_software: Software used to read this Fileset.
            reader_version: Version of software used to read this Fileset.
            keys: Keys of the associated files, the first entry is
                the entrypoint.
            import_uuid: UUID of the import.
            progress: Progress of importing the fileset

        Returns:
            The newly created Fileset.
        '''

        import_ = self.session.query(Import) \
            .filter(Import.uuid == import_uuid) \
            .one()
        fileset = Fileset(uuid, name, reader, reader_software, reader_version,
                          import_, progress)
        s3_keys = self.session.query(Key) \
            .filter(Key.import_uuid == import_uuid) \
            .filter(Key.key.in_(keys)) \
            .all()
        for key in s3_keys:
            if key.fileset is not None:
                raise DBError('Key is already used by another Fileset:'
                              f'{key.key}')
            key.fileset = fileset
        self.session.add(fileset)
        self.session.add_all(s3_keys)
        self.session.commit()
        return to_jsonapi(fileset_schema.dump(fileset))

    def create_image(self, uuid: str, name: str, pyramid_levels: int,
                     fileset_uuid: str = None, repository_uuid: str = None) -> SDict:
        '''Create image within the specified Fileset / Repository.

        Args:
            uuid : UUID of the image.
            name: Name of the import.
            pyramid_levels: Number of pyramid levels.
            fileset_uuid: UUID of the Fileset.
            repository_uuid: UUID of the Repository

        Returns:
            The newly create image.
        '''
        fileset = None
        if fileset_uuid is not None:
            fileset = self.session.query(Fileset) \
                .filter(Fileset.uuid == fileset_uuid) \
                .one()

        repository = None
        if repository_uuid is not None:
            repository = self.session.query(Repository).filter(Repository.uuid == repository_uuid).one()

        image = Image(uuid, name, pyramid_levels, fileset, repository)
        self.session.add(image)
        self.session.commit()
        return to_jsonapi(image_schema.dump(image))

    def create_rendering_settings(self, uuid:str, image_uuid: str, channels, label=None):
        image = self.session.query(Image).filter(Image.uuid == image_uuid).one()
        rendering_settings = RenderingSettings(uuid, image, channels, label)
        self.session.add(rendering_settings)
        self.session.commit()

    def update_rendering_settings(self, uuid:str, channels, label=None):
        rendering_settings = self.session.query(RenderingSettings).filter(RenderingSettings.uuid == str(uuid)).one()
        rendering_settings.label = label
        rendering_settings.channels = channels
        self.session.commit()

    def add_keys_to_import(self, keys: List[str], import_uuid: str) -> SDict:
        '''Create keys within the specified import.

        Args:
            keys: UUID keys of the files.
            import_uuid: UUID of the import.
        '''

        import_ = self.session.query(Import) \
            .filter(Import.uuid == import_uuid) \
            .one()

        s3_keys = [Key(key, import_=import_) for key in keys]

        self.session.add_all(s3_keys)
        self.session.commit()

    # TODO Is this used? It does not use serialized response
    # TODO Make grant creation more standalone with external exposure?
    # def grant_repository_to_subject(self, repository_uuid, subject_uuid,
    #                                 permission: str) -> SDict:
    #     '''Grant the specified repository to the specified subject.
    #
    #     Args:
    #         repository_uuid: UUID of the repository.
    #         subject_uuid: UUID of the subject.
    #         permission: Permission to grant the group
    #
    #     Returns:
    #         The grant that was created or updated.
    #     '''
    #
    #     # TODO Get from enumeration in model or potentially catch
    #     # psycopg2.DataError and rely on database to check
    #     if permission not in ['Read', 'Write', 'Admin']:
    #         raise ValueError(f'Specified permission invalid: {permission}')
    #
    #     grant = self.session.query(Grant) \
    #         .filter(Grant.repository_uuid == repository_uuid) \
    #         .filter(Grant.subject_uuid == subject_uuid) \
    #         .one_or_none()
    #
    #     # No existing grant exists
    #     if grant is None:
    #         repository = self.session.query(Repository) \
    #             .filter(Repository.uuid == repository_uuid) \
    #             .one()
    #
    #         subject = self.session.query(Subject) \
    #             .filter(Subject.uuid == subject_uuid) \
    #             .one()
    #
    #         grant = Grant(subject, repository, permission)
    #         self.session.add(grant)
    #
    #     # Grant exists, so potentially update permission
    #     else:
    #         grant.permission = permission
    #
    #     self.session.commit()
    #     return grant

    def get_fileset(self, uuid: str) -> SDict:
        '''Get details of the specified Fileset.

        Args:
            uuid: UUID of the Fileset.

        Returns:
            The Fileset details.
        '''

        return to_jsonapi(fileset_schema.dump(
            self.session.query(Fileset)
            .filter(Fileset.uuid == uuid)
            .one()
        ))

    def get_group(self, uuid: str) -> SDict:
        '''Get details of the specified group.

        Args:
            uuid: UUID of the group.

        Returns:
            The group details.
        '''

        return to_jsonapi(group_schema.dump(
            self.session.query(Group)
            .filter(Group.uuid == uuid)
            .one()
        ))

    def get_image(self, uuid: str) -> SDict:
        '''Get details of the specified image.

        Args:
            uuid: UUID of the image.

        Returns:
            The image details.
        '''
        image = self.session.query(Image).outerjoin(Image.rendering_settings) \
                .filter(Image.uuid == uuid) \
                .one()

        return to_jsonapi(image_schema.dump(image),
            {
                'rendering_settings': rendering_settings_schema.dump(image.rendering_settings)
            }
        )

    def get_image_channel_group(self, uuid: str):
        rendering_setting = self.session.query(RenderingSettings) \
            .filter(RenderingSettings.uuid == str(uuid)).one()
        return rendering_setting

    def get_import(self, uuid: str) -> SDict:
        '''Get details of the specified import.

        Args:
            uuid: UUID of the import.

        Returns:
            The import details.

        Raises:
            ValueError: If there is not exactly one matching import.
        '''

        return to_jsonapi(import_schema.dump(
            self.session.query(Import)
            .filter(Import.uuid == uuid)
            .one()
        ))

    def get_repository(self, uuid: str) -> SDict:
        '''Get details of the specified repository.

        Args:
            uuid: UUID of the repository.

        Returns:
            The repository details.
        '''

        return to_jsonapi(repository_schema.dump(
            self.session.query(Repository)
            .filter(Repository.uuid == uuid)
            .one()
        ))

    def get_user(self, uuid: str) -> SDict:
        '''Get details of the specified user.

        Args:
            uuid: UUID of the user.

        Returns:
            The user details.

        Raises:
            ValueError: If there is not exactly one matching user.
        '''

        return to_jsonapi(user_schema.dump(
            self.session.query(User)
            .filter(User.uuid == uuid)
            .one()
        ))

    def get_membership(self, group_uuid: str, user_uuid: str) -> SDict:
        '''Get details of the membership.

        Args:
            group_uuid: UUID of the group.
            user_uuid: UUID of the user.

        Returns:
            The membership details.

        Raises:
            ValueError: If there is not exactly one matching membership.
        '''

        membership = (
            self.session.query(Membership)
            .filter(Membership.group_uuid == group_uuid)
            .filter(Membership.user_uuid == user_uuid)
            .options(
                joinedload(Membership.group),
                joinedload(Membership.user)
            )
            .one()
        )

        return to_jsonapi(
            membership_schema.dump(membership),
            {
                'groups': [group_schema.dump(membership.group)],
                'users': [user_schema.dump(membership.user)]
            }
        )

    def is_member(self, group_uuid: str, user_uuid: str,
                  membership_type: Optional[str] = 'Member') -> bool:
        '''Determine if a user is a member of a group.

        Args:
            group_uuid: UUID of the group.
            user_uuid: UUID of the user.
            membership_type: The required membership type.

        Returns:
            If user is a member of the given membership level (or that is
            implied by a level of membership that the member does have) or not.
        '''

        # TODO Calculate this centrally driven from the model
        # Calculate the memberships which imply the requested one
        implied = [membership_type]
        if membership_type == 'Member':
            implied.append('Owner')

        q = (
            self.session.query(Membership)
            .filter(Membership.group_uuid == group_uuid)
            .filter(Membership.user_uuid == user_uuid)
            .filter(Membership.membership_type.in_(implied))
            .exists()
        )

        return self.session.query(q).scalar()

    def is_owner(self, group_uuid: str, user_uuid: str) -> bool:
        '''Determine if a user is a member of a group.

        Args:
            group_uuid: UUID of the group.
            user_uuid: UUID of the user.

        Returns:
            If user is a member or not.
        '''

        return self.is_member(group_uuid, user_uuid, 'Owner')

    def has_permission(self, user_uuid: str, resource_type: str,
                       resource_uuid: str,
                       permission: Optional[str] = 'Read') -> bool:
        '''Determine if a user has a required permission on a given resource.

        Args:
            user_uuid: UUID of the user.
            resource_type: Type of the resource.
            resource_uuid: UUID of the resource.
            permission: Sought permission. Defaults to 'Read'

        Returns:
            If user has permission or not.
        '''

        # TODO Calculate this centrally driven from the model
        # Caclculate the permissions which imply the requested one
        implied = [permission]
        if permission == 'Read':
            implied.extend(['Write', 'Admin'])
        if permission == 'Write':
            implied.append('Admin')

        # Calculate subjects that the user is a member of that might have the
        # required permission granted
        q_subject_uuids = premade.q_subject_uuids(self.session, user_uuid)

        # Base query
        q = (
            self.session.query(Grant)
            .filter(Grant.permission.in_(implied))
            .filter(Grant.subject_uuid.in_(q_subject_uuids))
        )

        if resource_type == 'Repository':
            q = q.filter(Grant.repository_uuid == resource_uuid)

        elif resource_type == 'Import':
            q = (
                q.join(Grant.repository)
                .join(Repository.imports)
                .filter(Import.uuid == resource_uuid)
            )
        elif resource_type == 'Fileset':
            q = (
                q.join(Grant.repository)
                .join(Repository.imports)
                .join(Import.filesets)
                .filter(Fileset.uuid == resource_uuid)
            )
        elif resource_type == 'Image':
            q = (
                q.join(Grant.repository)
                .join(Repository.images)
                .filter(Image.uuid == resource_uuid)
            )

        # Only existance of results required
        q = q.exists()

        return self.session.query(q).scalar()

    # TODO Should be list grants?
    def list_repositories_for_user(
        self,
        uuid: str,
        implied: Optional[bool] = False
    ) -> List[SDict]:
        '''List repositories that a user is a member of (with permissions).

        Args:
            uuid: UUID of the user.
            implied: Include repositories implied through group membership.
                Default: False.

        Returns:
            The list of repositories the user is a member of along with the
            permissions that the user has.
        '''

        q_subject_uuids = premade.q_subject_uuids(self.session, uuid)
        q = (
            self.session.query(Grant)
            .join(Grant.repository)
            .filter(Grant.subject_uuid.in_(q_subject_uuids))
        )
        grants = q.all()
        repositories = [grant.repository for grant in grants]

        return to_jsonapi(
            grants_schema.dump(grants),
            {
                'repositories': repositories_schema.dump(repositories)
            }
        )

    def list_imports_in_repository(self, uuid: str) -> List[SDict]:
        '''List imports in given repository.

        Args:
            uuid: UUID of the repository.

        Returns:
            The list of imports in the repository.
        '''

        return to_jsonapi(imports_schema.dump(
            self.session.query(Import)
            .filter(Import.repository_uuid == uuid)
            .all()
        ))

    def list_filesets_in_import(self, uuid: str) -> List[SDict]:
        '''List Filesets in given import.

        Args:
            uuid: UUID of the import.

        Returns:
            The list of Filesets in the import.
        '''

        return to_jsonapi(filesets_schema.dump(
            self.session.query(Fileset)
            .filter(Fileset.import_uuid == uuid)
            .all()
        ))

    def list_keys_in_import(self, uuid: str) -> List[SDict]:
        '''List keys in given import.

        Args:
            uuid: UUID of the import.

        Returns:
            The list of keys in the import.
        '''

        return to_jsonapi(keys_schema.dump(
            self.session.query(Key)
            .filter(Key.import_uuid == uuid)
            .all()
        ))

    def list_images_in_fileset(self, uuid: str) -> List[SDict]:
        '''List images in given Fileset.

        Args:
            uuid: UUID of the Fileset.

        Returns:
            The list of images in the Fileset.
        '''

        return to_jsonapi(images_schema.dump(
            self.session.query(Image)
            .filter(Image.fileset_uuid == uuid)
            .all()
        ))

    def list_images_in_repository(self, repository_uuid: str) -> List[SDict]:
        return to_jsonapi(images_schema.dump(
            self.session.query(Image)
            .filter(Image.repository_uuid == repository_uuid)
            .all()
        ))

    def list_keys_in_fileset(self, uuid: str) -> List[SDict]:
        '''List keys in given Fileset.

        Args:
            uuid: UUID of the Fileset.

        Returns:
            The list of keys in the Fileset.
        '''

        return to_jsonapi(keys_schema.dump(
            self.session.query(Key)
            .filter(Key.fileset_uuid == uuid)
            .all()
        ))

    def list_incomplete_imports(self) -> List[SDict]:

        results = self.session.query(Import, Fileset).outerjoin(Fileset) \
            .filter(Fileset == None or Fileset.complete == False) \
            .all()

        imports = []
        filesets = []
        for result in results:
            for entity in result:
                if isinstance(entity, Import):
                    imports.append(entity)
                elif isinstance(entity, Fileset):
                    filesets.append(entity)

        return to_jsonapi(
            grants_schema.dump(imports),
            {
                'filesets': filesets_schema.dump(filesets)
            }
        )


    def list_rendering_settings(self, image_uuid: str):
        rendering_settings = self.session.query(RenderingSettings) \
            .filter(RenderingSettings.image_uuid == image_uuid).all()
        channel_groups = []
        for setting in rendering_settings:
            channel_groups.append(setting)
        return channel_groups

    def update_import(self, uuid: str, name: Optional[str] = None,
                      complete: Optional[bool] = None) -> SDict:
        '''Update a import.

        Args:
            uuid: UUID of the import.
            name: Updated name of the import. Default: `None` for no
                update.
            complete: Updated completedness of the import. Default: `None`
                for no update.

        Returns:
            The updated import.
        '''

        import_ = (
            self.session.query(Import)
            .filter(Import.uuid == uuid)
            .one()
        )

        if name is not None:
            import_.name = name

        if complete is not None:
            import_.complete = complete

        self.session.add(import_)
        self.session.commit()
        return to_jsonapi(import_schema.dump(import_))

    def update_fileset(self, uuid: str, name: Optional[str] = None,
                       complete: Optional[bool] = None,
                       images: Optional[List[SDict]] = None,
                       progress: Optional[int] = 0) -> SDict:
        '''Update a Fileset.

        Args:
            uuid: UUID of the Fileset.
            name: Updated name of the Fileset. Default: `None` for no update.
            complete: Updated completedness of the Fileset. Default: `None`
                for no update.
            images: Updated list of images to register to the Fileset.
            progress: Progress of the fileset import (0-100)

        Returns:
            The updated Fileset.
        '''

        fileset = (
            self.session.query(Fileset).outerjoin(Import)
            .filter(Fileset.uuid == uuid)
            .one()
        )

        if name is not None:
            fileset.name = name

        if complete is not None:
            fileset.complete = complete

        if progress is not None:
            fileset.progress = progress

        if images is not None:
            if fileset.complete is False:
                raise DBError('Images can only be registered to a completed '
                              'Fileset.')

            images = [Image(**image, fileset=fileset, repository=fileset.import_.repository) for image in images]
            self.session.add_all(images)

        self.session.add(fileset)
        self.session.commit()
        return to_jsonapi(fileset_schema.dump(fileset))

    def update_repository(self, uuid: str, name: Optional[str] = None,
                          raw_storage: Optional[str] = None) -> SDict:
        '''Update a repository.

        Args:
            uuid: UUID of the repository.
            name: Updated name of the repository. Default: `None` for no
                update.
            raw_storage: Updated storage level of the raw data. Default: `None`
                for no update.

        Returns:
            The updated repository.
        '''

        repository = (
            self.session.query(Repository)
            .filter(Repository.uuid == uuid)
            .one()
        )

        if name is not None:
            repository.name = name

        if raw_storage is not None:
            repository.raw_storage = raw_storage

        # TODO Handle storage level retrospectively in the calling method
        # Live -> Archive (Tag all objects as project:archive to lifecycle)
        # Live/Archive -> Destroy (Remove all objects)
        # Archive -> Live (Restore data from Glacier then copy the object to
        #   the same key without project:archive tag to change storage level
        #   permanently)
        # Destroy -> Live/Archive (Data will be missing)
        # Potentially use lifecycle to delete also to protect from mistakes?
        self.session.add(repository)
        self.session.commit()
        return to_jsonapi(repository_schema.dump(repository))

    def update_membership(self, group_uuid: str, user_uuid: str,
                          membership_type: Optional[str] = None) -> SDict:
        '''Update a membership.

        Args:
            group_uuid: UUID of the group.
            user_uuid: UUID of the user.
            membership_type: Membership level the member shall be given.

        Returns:
            The updated membership.
        '''

        membership = (
            self.session.query(Membership)
            .filter(Membership.group_uuid == group_uuid)
            .filter(Membership.user_uuid == user_uuid)
            .options(
                joinedload(Membership.group),
                joinedload(Membership.user)
            )
            .one()
        )

        if membership_type is not None:
            membership.membership_type = membership_type

        self.session.add(membership)
        self.session.commit()
        return to_jsonapi(
            membership_schema.dump(membership),
            {
                'groups': [group_schema.dump(membership.group)],
                'users': [user_schema.dump(membership.user)]
            }
        )

    def delete_repository(self, uuid: str):
        '''Delete a repository and all contents.

        Args:
            uuid: UUID of the repository.
        '''

        repository = (
            self.session.query(Repository)
            .filter(Repository.uuid == uuid)
            .one()
        )

        # TODO Handle delete of raw/tiled objects in the calling method
        # Recovery from delete?
        self.session.delete(repository)
        self.session.commit()

    def delete_image(self, uuid: str):
        image = (
            self.session.query(Image)
            .filter(Image.uuid == uuid)
            .one()
        )
        # TODO Handle delete of raw/tiled objects in a batch job
        image.deleted = True
        self.session.commit()

    def restore_image(self, uuid: str):
        image = (
            self.session.query(Image)
            .filter(Image.uuid == uuid)
            .one()
        )
        image.deleted = False
        self.session.commit()

    def delete_membership(self, group_uuid: str, user_uuid: str):
        '''Delete a membership.

        Args:
            group_uuid: UUID of the group.
            user_uuid: UUID of the user.
        '''

        membership = (
            self.session.query(Membership)
            .filter(Membership.group_uuid == group_uuid)
            .filter(Membership.user_uuid == user_uuid)
            .one()
        )

        self.session.delete(membership)
        self.session.commit()
