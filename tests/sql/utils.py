from minerva_db.sql.models import Base
from typing import Dict, List, Type, Union


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
