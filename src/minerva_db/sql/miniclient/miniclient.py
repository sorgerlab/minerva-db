from minerva_db.sql.models.membership import Membership
from minerva_db.sql.models.repository import Repository
from minerva_db.sql.models.image import Image
from minerva_db.sql.models.grant import Grant
from minerva_db.sql.models.renderingsettings import RenderingSettings
from sqlalchemy.sql.expression import literal

# Minimal database client for tile rendering API
# The goal is to have minimal functionality / imports so that the
# lambda cold start time of the tile rendering API would be smaller.
# (api.client.Client imports everything, which takes too much time)
class MiniClient:

    def __init__(self, session):
        self.session = session

    def _session(self):
        '''Get session.

        Returns:
            The client's session.
        '''

        return self.session

    def q_subject_uuids(self, session, user_uuid):
        '''
        Query for subject_ids that apply to a user. This is all
        the IDs of groups they belong to, and their own ID.
        '''

        # The groups a user is a member of
        q_groups = session.query(
            Membership.group_uuid.label('subject_uuid')
        ).filter(
            Membership.user_uuid == user_uuid
        )

        # We already know the user id so simply add this
        q_user = session.query(
            literal(user_uuid).label('subject_uuid')
        )

        return q_groups.union(q_user)

    def has_image_permission(self, user_uuid: str,
                       image_uuid: str,
                       permission='Read') -> bool:
        '''Determine if a user has a required permission on a given image.

        Args:
            user_uuid: UUID of the user.
            image_uuid: UUID of the Image.
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
        q_subject_uuids = self.q_subject_uuids(self.session, user_uuid)

        # Base query
        q = (
            self.session.query(Grant)
                .filter(Grant.permission.in_(implied))
                .filter(Grant.subject_uuid.in_(q_subject_uuids))
        )

        q = (
            q.join(Grant.repository)
                .join(Repository.images)
                .filter(Image.uuid == image_uuid)
        )

        # Only existence of results required
        q = q.exists()

        return self.session.query(q).scalar()

    def get_image_channel_group(self, uuid: str):
        rendering_setting = self.session.query(RenderingSettings) \
            .filter(RenderingSettings.uuid == str(uuid)).one()
        return rendering_setting
