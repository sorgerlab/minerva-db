import os
import pkg_resources
from typing import Dict, List, Optional
from .connection import Connection

PREFIX = pkg_resources.resource_string(
    __name__,
    os.path.join('schema', 'prefix.rq')
).decode('utf-8')

SCHEMA = pkg_resources.resource_string(
    __name__,
    os.path.join('schema', 'schema.rq')
).decode('utf-8')


class Client():

    def __init__(self, entrypoint: str, entrypoint_ro: Optional[str] = None):
        self.conn = Connection(entrypoint, entrypoint_ro)

    def _connection(self) -> Connection:
        '''Get connection.

        Returns:
            The client's connection.
        '''

        return self.conn

    def _init_db(self, extra: Optional[str] = None):
        '''Clear and initialise the database.

        Args:
            extra: Optional statement to execute. Does not get prefixed.

        Not to be used by ordinary client usage.
        '''

        self.conn.update(PREFIX + 'DELETE WHERE { ?s ?p ?o }')
        self.conn.update(PREFIX + SCHEMA)
        if extra is not None:
            self.conn.update(extra)

    def add_files_to_import(self, keys: List[str], import_: str):
        '''Create files within the specified import.

        Args:
            keys: UUID keys of the files.
            import_: UUID of the import.
        '''

        keys = ''.join(['''
            <{0}> rdf:type :File ;
               :key "{0}" ;
               :inResource ?import .
        '''.format(key) for key in keys])

        statement = PREFIX + '''
            INSERT {
                %s
            } WHERE {
                BIND (d:%s AS ?import)
                ?import rdf:type :Import .
            }
        ''' % (keys, import_)

        self.conn.update(statement)

    def add_users_to_group(self, group: str, users: List[str]):
        '''Add users to the specified group.

        Args:
            group: UUID of the group.
            users: UUIDs of the users.
        '''

        users = ' '.join(['cup:{}'.format(user) for user in users])

        statement = PREFIX + '''
            INSERT {
                ?user :memberOf ?group .
            } WHERE {
                BIND (d:%s AS ?group)
                ?group rdf:type :Group .
                ?user rdf:type :User .
                VALUES ?user { %s }
            }
        ''' % (group, users)

        self.conn.update(statement)

    def create_bfu(self, uuid: str, name: str, reader: str,
                   keys: List[str], import_: str):
        '''Create a BFU within the specified import.

        Associates the given files.

        Args:
            uuid: UUID of the BFU.
            name: Name of the BFU.
            reader: Bio-Formats reader used to read the BFU.
            keys: Keys of the associated files, the first entry is
                the entrypoint.
            import_: UUID of the import.
        '''

        keys = ['<{}>'.format(key) for key in keys]
        entrypoint = keys[0]
        keys = ' '.join(keys)

        statement = PREFIX + '''
            INSERT {
                ?bfu rdf:type :BFU ;
                     :name "%s" ;
                     :reader "%s" ;
                     :inResource ?import .
                ?file :inResource ?bfu .
                ?entrypoint :entrypoint true .
            } WHERE {
                BIND (d:%s AS ?bfu)
                BIND (d:%s AS ?import)
                BIND (%s AS ?entrypoint)
                ?import rdf:type :Import .
                ?file rdf:type :File ;
                      :inResource ?import .
                VALUES ?file { %s }
                ?entrypoint rdf:type :File ;
                            :inResource ?import .
            }
        ''' % (name, reader, uuid, import_, entrypoint, keys)

        self.conn.update(statement)

    def create_image(self, uuid: str, name: str, key: str, pyramid_levels: int,
                     bfu: str):
        '''Create image within the specified BFU.

        Args:
            uuid : UUID of the image.
            name: Name of the import.
            key: Prefix key of the image.
            pyramid_levels: Number of pyramid levels.
            bfu: UUID of the BFU.
        '''

        statement = PREFIX + '''
            INSERT {
                d:%s rdf:type :Image ;
                     :name "%s" ;
                     :key "%s" ;
                     :pyramidLevels %d ;
                     :inResource ?bfu .
            } WHERE {
                BIND (d:%s AS ?bfu)
                ?bfu rdf:type :BFU .
            }
        ''' % (uuid, name, key, pyramid_levels, bfu)

        self.conn.update(statement)

    def create_import(self, uuid: str, name: str, key: str, repository: str):
        '''Create an import within the specified repository.

        Args:
            uuid: UUID of the import.
            name: Name of the import.
            key: Prefix key of the import.
            repository: UUID of the repository.
        '''

        statement = PREFIX + '''
            INSERT {
                d:%s rdf:type :Import ;
                     :name "%s" ;
                     :key "%s" ;
                     :complete false ;
                     :inResource ?repo .
            } WHERE {
                BIND (d:%s AS ?repo)
                ?repo rdf:type :Repository .
            }
        ''' % (uuid, name, key, repository)

        self.conn.update(statement)

    def create_group(self, uuid: str, name: str, user: str):
        '''Create a group with the specified user as a member.

        Args:
            uuid: UUID of the group.
            name: Name of the group.
            user: UUID of the user to be the initial member.
        '''

        statement = PREFIX + '''
            INSERT {
                ?group rdf:type :Group ;
                       :name "%s" .
                ?user :memberOf ?group .
            } WHERE {
                BIND(d:%s AS ?group)
                BIND(cup:%s AS ?user)
                ?user rdf:type :User .
            }
        ''' % (name, uuid, user)

        self.conn.update(statement)

    def create_repository(self, uuid: str, name: str, user: str):
        '''Create a repository with the specified user as an admin.

        Args:
            uuid: UUID of the repository.
            name: Name of the repository.
            user: UUID of the user to be initial admin.
        '''

        statement = PREFIX + '''
            INSERT {
                ?repository rdf:type :Repository ;
                            :name "%s" .
                ?user :Admin ?repository .
            } WHERE {
                BIND (d:%s AS ?repository)
                BIND (cup:%s AS ?user)
                ?user rdf:type :User .
            }
        ''' % (name, uuid, user)

        self.conn.update(statement)

    def create_user(self, uuid: str, name: str, email: str):
        '''Create a user.

        Args:
            uuid: UUID of the user.
            name: Name of the user.
            email: Email of the user.
        '''

        statement = PREFIX + '''
            INSERT DATA {
                cup:%s rdf:type :User ;
                       :name "%s" ;
                       :email "%s" .
            }
        ''' % (uuid, name, email)

        self.conn.update(statement)

    def describe_bfu(self, uuid: str) -> Dict[str, str]:
        '''Get details of the specified BFU.

        Args:
            uuid: UUID of the BFU.

        Returns:
            The BFU details.

        Raises:
            ValueError: If there is not exactly one matching BFU.
        '''

        statement = PREFIX + '''
            SELECT ?name ?reader ?import (?key AS ?entrypoint)
            WHERE {
                BIND (d:%s AS ?bfu)
                ?bfu rdf:type :BFU ;
                     :name ?name ;
                     :reader ?reader ;
                     :inResource ?import .
                ?import rdf:type :Import .
                ?file rdf:type :File ;
                      :inResource ?bfu ;
                      :entrypoint true ;
                      :key ?key .
            }
        ''' % uuid

        header, rows = self.conn.query(statement)
        if len(rows) == 0:
            raise ValueError('BFU ({}) not found'.format(uuid))

        row = rows[0]

        # Remove the prefix
        row['import'] = row['import'].split('#')[1]

        return row

    def describe_group(self, uuid: str) -> Dict[str, str]:
        '''Get details of the specified group.

        Args:
            uuid: UUID of the group.

        Returns:
            The group details.

        Raises:
            ValueError: If there is not exactly one matching group.
        '''

        statement = PREFIX + '''
            SELECT ?name
            WHERE {
                d:%s rdf:type :Group ;
                     :name ?name .
            }
        ''' % uuid

        header, rows = self.conn.query(statement)
        if len(rows) == 0:
            raise ValueError('Group ({}) not found'.format(uuid))

        return rows[0]

    def describe_image(self, uuid: str) -> Dict[str, str]:
        '''Get details of the specified image.

        Args:
            uuid: UUID of the image.

        Returns:
            The image details.

        Raises:
            ValueError: If there is not exactly one matching image.
        '''

        statement = PREFIX + '''
            SELECT ?name ?key ?pyramidLevels ?bfu
            WHERE {
                d:%s rdf:type :Image ;
                      :name ?name ;
                      :key ?key ;
                      :pyramidLevels ?pyramidLevels ;
                      :inResource ?bfu .
                ?bfu rdf:type :BFU .
            }
        ''' % uuid

        header, rows = self.conn.query(statement)
        if len(rows) == 0:
            raise ValueError('Image ({}) not found'.format(uuid))

        row = rows[0]

        # Remove the prefix
        row['bfu'] = row['bfu'].split('#')[1]

        # Parse the int
        row['pyramidLevels'] = int(row['pyramidLevels'])

        return row

    def describe_import(self, uuid: str) -> Dict[str, str]:
        '''Get details of the specified import.

        Args:
            uuid: UUID of the import.

        Returns:
            The import details.

        Raises:
            ValueError: If there is not exactly one matching import.
        '''

        statement = PREFIX + '''
            SELECT ?name ?key ?complete ?repository
            WHERE {
                d:%s rdf:type :Import ;
                     :name ?name ;
                     :key ?key ;
                     :complete ?complete ;
                     :inResource ?repository .
                ?repository rdf:type :Repository .
            }
        ''' % uuid

        header, rows = self.conn.query(statement)
        if len(rows) == 0:
            raise ValueError('Import ({}) not found'.format(uuid))

        row = rows[0]

        # Remove the prefix
        row['repository'] = row['repository'].split('#')[1]

        # Convert complete to boolean
        row['complete'] = row['complete'] == 'true'

        return row

    def describe_repository(self, uuid: str) -> Dict[str, str]:
        '''Get details of the specified repository.

        Args:
            uuid: UUID of the repository.

        Returns:
            The repository details.

        Raises:
            ValueError: If there is not exactly one matching repository.
        '''

        statement = PREFIX + '''
            SELECT ?name
            WHERE {
                d:%s rdf:type :Repository ;
                     :name ?name .
            }
        ''' % uuid

        header, rows = self.conn.query(statement)
        if len(rows) == 0:
            raise ValueError('Repository ({}) not found'.format(uuid))

        return rows[0]

    def describe_user(self, uuid: str) -> Dict[str, str]:
        '''Get details of the specified user.

        Args:
            uuid: UUID of the user.

        Returns:
            The user details.

        Raises:
            ValueError: If there is not exactly one matching group.
        '''

        statement = PREFIX + '''
            SELECT ?name ?email
            WHERE {
                BIND (cup:%s AS ?user)
                ?user rdf:type :User ;
                       :name ?name ;
                OPTIONAL {
                    ?user :email ?email .
                }
            }
        ''' % uuid

        header, rows = self.conn.query(statement)
        if len(rows) == 0:
            raise ValueError('User ({}) not found'.format(uuid))

        return rows[0]

    def has_user_permission(self, user: str, resource: str = None,
                            permission: Optional[str] = 'Read',
                            raw_resource: Optional[bool] = False) -> bool:
        '''Determine if a user has a permission on a resource.

        Args:
            user: UUID of the user.
            resource: UUID of the resource or URI if _raw_resource_ set to
                True.
            permission: Sought permission. Defaults to 'Read'
            raw_resource: Indicates the resource should not have a prexix
                added. Defaults to False.

        Returns:
            If user has permission or not.
        '''

        # Prefix/parenthesize resource as necessary
        if raw_resource is True:
            resource = '<{}>'.format(resource)
        else:
            resource = 'd:{}'.format(resource)

        statement = PREFIX + '''
            ASK {
                BIND(cup:%s AS ?user)
                ?subjectType rdfs:subClassOf :Subject .
                ?subject rdf:type ?subjectType .
                ?user :memberOf* ?subject .

                BIND(%s AS ?targetResource)
                ?resourceType rdfs:subClassOf :Resource .
                ?resource rdf:type ?resourceType .
                ?targetResource :inResource* ?resource .

                BIND(:%s AS ?targetPermission)
                ?permissionType rdfs:subPropertyOf :Permission .
                ?permission :implies* ?targetPermission .

                ?subject ?permission ?resource .
            }
        ''' % (user, resource, permission)

        return self.conn.ask(statement)

    def is_member(self, user: str, group: str = None) -> bool:
        '''Determine if a user is a member of a group.

        User can be a member of a group.
        User can be a member of a group that is a member of a group, etc.

        Args:
            user: UUID of the user.
            group: UUID of the group.

        Returns:
            If user is a member or not.
        '''

        statement = PREFIX + '''
            ASK {
                BIND(cup:%s AS ?user)
                BIND(d:%s AS ?group)
                ?group rdf:type :Group .
                ?user :memberOf+ ?group .
            }
        ''' % (user, group)

        return self.conn.ask(statement)

    def list_bfus_in_import(self, uuid: str) -> List[Dict[str, str]]:
        '''List BFUs in the specified import.

        Args:
            uuid: UUID of the import.

        Returns:
            The list of BFUs (with details) in the import.
        '''

        statement = PREFIX + '''
            SELECT (?bfu AS ?uuid) ?name ?reader (?key AS ?entrypoint)
            WHERE {
                BIND (d:%s AS ?import)
                ?import rdf:type :Import .
                ?bfu rdf:type :BFU ;
                     :inResource ?import ;
                     :name ?name ;
                     :reader ?reader .
                ?file rdf:type :File ;
                      :inResource ?bfu ;
                      :entrypoint true ;
                      :key ?key .
            }
        ''' % uuid

        header, rows = self.conn.query(statement)

        # Remove the prefixes
        for row in rows:
            row['uuid'] = row['uuid'].split('#')[1]

        return rows

    def list_files_in_bfu(self, uuid: str) -> List[Dict[str, str]]:
        '''List files in the specified BFU.

        Args:
            uuid: UUID of the BFU.

        Returns:
            The list of files (with details) in the BFU.
        '''

        statement = PREFIX + '''
            SELECT ?key ?entrypoint
            WHERE {
                BIND (d:%s AS ?bfu)
                ?bfu rdf:type :BFU .
                ?file rdf:type :File ;
                      :inResource ?bfu ;
                      :key ?key .
                OPTIONAL { ?file :entrypoint ?entrypoint }
            }
        ''' % uuid

        header, rows = self.conn.query(statement)

        # Set entrypoint to always have a boolean value
        for row in rows:
            if 'entrypoint' in row:
                row['entrypoint'] = row['entrypoint'] == 'true'
            else:
                row['entrypoint'] = False

        return rows

    def list_files_in_import(self, uuid: str) -> List[Dict[str, str]]:
        '''List files in the specified import.

        Args:
            uuid: UUID of the import.

        Returns:
            The list of files (with details) in the import.
        '''

        statement = PREFIX + '''
            SELECT ?key
            WHERE {
                BIND (d:%s AS ?import)
                ?import rdf:type :Import .
                ?file rdf:type :File ;
                      :inResource ?import ;
                      :key ?key .
            }
        ''' % uuid

        header, rows = self.conn.query(statement)

        return rows

    def list_imports_in_repository(self, uuid: str) -> List[Dict[str, str]]:
        '''List imports in the specified repository.

        Args:
            uuid: UUID of the repository.

        Returns:
            The list of imports (with details) in the repository.
        '''

        statement = PREFIX + '''
            SELECT (?import AS ?uuid) ?name ?key ?complete
            WHERE {
                BIND (d:%s AS ?repository)
                ?repository rdf:type :Repository .
                ?import rdf:type :Import ;
                        :inResource ?repository ;
                        :name ?name ;
                        :key ?key ;
                        :complete ?complete .
            }
        ''' % uuid

        header, rows = self.conn.query(statement)

        for row in rows:
            # Remove the prefix
            row['uuid'] = row['uuid'].split('#')[1]

            # Convert complete to boolean
            row['complete'] = row['complete'] == 'true'

        return rows

    def list_users_in_group(self, uuid: str) -> List[Dict[str, str]]:
        '''List users in the specified group.

        Args:
            uuid: UUID of the group.

        Returns:
            The list of users (with details) in the group.
        '''

        statement = PREFIX + '''
            SELECT (?user as ?uuid) ?name ?email
            WHERE {
                BIND (d:%s AS ?group)
                ?group rdf:type :Group .
                ?user rdf:type :User ;
                      :memberOf ?group ;
                      :name ?name ;
                      :email ?email .
            }
        ''' % uuid

        header, rows = self.conn.query(statement)

        # Remove the prefixes
        for row in rows:
            row['uuid'] = row['uuid'].split('#')[1]

        return rows

    def set_import_complete(self, uuid: str, complete: Optional[bool] = True):
        '''Mark an import as complete.

        Args:
            uuid: UUID of the import.
        '''

        statement = PREFIX + '''
            DELETE { ?import :complete ?prevComplete }
            INSERT { ?import :complete ?newComplete }
            WHERE {
                BIND(d:%s AS ?import)
                BIND(%s AS ?prevComplete)
                BIND(%s AS ?newComplete)
                ?import rdf:type :Import ;
                        :complete ?prevComplete
            }

        ''' % (uuid, str(not complete).lower(), str(complete).lower())

        self.conn.update(statement)
