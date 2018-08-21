from contextlib import contextmanager
from minerva_db.sql.models import Base
from sqlalchemy import event
from sqlalchemy.engine import Connection
from sqlalchemy.sql.base import Executable
from typing import Dict, List, Type, Union


# TODO Refine type of Dict


def sa_obj_to_dict(obj: Type[Base],
                   keys: List[str]) -> Dict[str, Union[int, str, float]]:
    '''Convert a SQL Alchemy object into a dictionary with the given keys.

    Args:
        obj: The SQL Alchemy object.
        keys: The keys to include in the returned dictionary.

    Returns:
        A dictionary.
    '''

    obj_dict = obj.as_dict()
    return {key: obj_dict[key] for key in keys}


@contextmanager
def statement_log(connection: Connection) -> List[Type[Executable]]:
    '''For the duration of this context, record the executed statements.

    Args:
        connection: The SQL Alchemy Connection.

    Yields:
        The current list of statements.
    '''

    statements = []

    def before_execute(conn, clauseelement, multiparams, params):
        statements.append(clauseelement)

    event.listen(connection, 'before_execute', before_execute)
    try:
        yield statements
    finally:
        event.remove(connection, "before_execute", before_execute)
