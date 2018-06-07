'''Premade statements'''
from sqlalchemy.sql.expression import literal
from ..models import Membership


def q_subject_uuids(session, user_uuid):
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
