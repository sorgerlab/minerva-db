from contextlib import contextmanager
from minerva_db.sql.models import Base
from sqlalchemy import event
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
def statement_log(connection):

    class StatementsBase():
        def __init__(self):
            self.statements = []

        def before_execute(self, conn, clauseelement, multiparams, params):
            self.statements.append(clauseelement)

        def attach(self):
            event.listen(connection, 'before_execute', self.before_execute)

        def dettach(self):
            event.remove(connection, "before_execute", self.before_execute)
            self.statements = []

    statements_base = StatementsBase()
    statements_base.attach()
    try:
        yield statements_base.statements
    finally:
        statements_base.dettach()
