with open('prefix.rq', 'r') as p:
    PREFIX = p.read()


def schema():

    with open('prefix.rq', 'r') as p, open('schema.rq', 'r') as s:
        statement = p.read() + s.read()
        return statement


def sample_data():

    with open('prefix.rq', 'r') as p, open('test.rq', 'r') as t:
        statement = p.read() + t.read()
        return statement


def query_all_triples():

    return '''
        SELECT ?s ?p ?o
        WHERE { ?s ?p ?o . }
    '''


def delete_all_triples():

    return '''
        DELETE WHERE { ?s ?p ?o }
    '''


# def create_user(uuid, name, email):
#     return PREFIX + '''
#         INSERT DATA {
#             cup:%s rdf:type :User ;
#                    :name "%s" ;
#                    :email "%s" .
#         }
#     ''' % (uuid, name, email)


def query_user_details(uuid):
    return PREFIX + '''
        SELECT ?name ?email
        WHERE {
            cup:%s rdf:type :User ;
                   :name ?name ;
            OPTIONAL {
                cup:%s :email ?email .
            }
        }
    ''' % (uuid, uuid)


def create_group(uuid, name):
    return PREFIX + '''
        INSERT DATA {
            d:%s rdf:type :Group ;
                   :name "%s" .
        }
    ''' % (uuid, name)


def query_group_details(uuid):
    return PREFIX + '''
        SELECT ?name
        WHERE {
            d:%s rdf:type :Group ;
                 :name ?name .
        }
    ''' % uuid


def query_group_members(uuid):
    return PREFIX + '''
        SELECT ?user ?name
        WHERE {
            BIND(d:%s AS ?group)
            ?group rdf:type :Group .
            ?user rdf:type :User ;
                  :memberOf ?group ;
                  :name ?name .
        }
    ''' % uuid


def add_users_to_group(group, users):

    users = ' '.join(['cup:{}'.format(user) for user in users])
    return PREFIX + '''
        INSERT {
            ?user :memberOf ?group .
        } WHERE {
            {
                SELECT ?group
                WHERE {
                    BIND (d:%s AS ?group)
                    ?group rdf:type :Group .
                }
            }
            {
                SELECT ?user
                WHERE {
                    ?user rdf:type :User .
                    VALUES ?user { %s }
                }
            }
        }
    ''' % (group, users)


def create_repository(uuid, name, user):
    '''Create a repository with the specified user as an admin

    Args:
        uuid (:obj:`str`): UUID of the repository
        name (:obj:`str`): Name of the repository
        user (:obj:`str`): UUID of the user to be initial admin
    '''

    return PREFIX + '''
        INSERT {
            ?repo rdf:type :Repository ;
                  :name "%s" .
            ?user :Admin ?repo .
        } WHERE {
            BIND (d:%s AS ?repo)
            BIND (cup:%s AS ?user)
            ?user rdf:type :User .
        }
    ''' % (name, uuid, user)


def query_repository_details(uuid):
    '''Query details of the specified repository

    Args:
        uuid (:obj:`str`): UUID of the repository
    '''

    return PREFIX + '''
        SELECT ?name
        WHERE {
            d:%s rdf:type :Repository ;
                 :name ?name .
        }
    ''' % uuid


def create_import(uuid, name, key, repository):
    '''Create a repository within the specified repository

    Args:
        uuid (:obj:`str`): UUID of the import
        name (:obj:`str`): Name of the import
        key (:obj:`str`): Prefix key of the import
        repository (:obj:`str`): UUID of the repository
    '''

    return PREFIX + '''
        INSERT {
            d:%s rdf:type :Import ;
                 :name "%s" ;
                 :key "%s" ;
                 :inRepository ?repo .
        } WHERE {
            BIND (d:%s AS ?repo)
            ?repo rdf:type :Repository .
        }
    ''' % (uuid, name, key, repository)


def query_import_details(uuid):
    '''Query details of the specified import

    Args:
        uuid (:obj:`str`): UUID of the import
    '''

    return PREFIX + '''
        SELECT ?name ?key
        WHERE {
            d:%s rdf:type :Import ;
                 :name ?name ;
                 :key ?key .
        }
    ''' % uuid


def add_files_to_import(keys, import_):
    '''Create files within the specified import

    Args:
        keys (:obj:`str`): UUID keys of the files
        import_ (:obj:`str`): UUID of the import
    '''

    keys = ''.join(['''

        <{1}> rdf:type :File ;
           :key "{1}" ;
           :inImport ?import .

    '''.format(i, key) for i, key in enumerate(keys)])

    return PREFIX + '''
        INSERT {
            %s
        } WHERE {
            BIND (d:%s AS ?import)
            ?import rdf:type :Import .
        }
    ''' % (keys, import_)


def query_import_files(uuid):
    '''Query files in the specified import

    Args:
        uuid (:obj:`str`): UUID of the import
    '''

    return PREFIX + '''
        SELECT ?key
        WHERE {
            ?file rdf:type :File ;
                  :key ?key ;
                  :inImport d:%s .
        }
    ''' % uuid


def create_bfu(uuid, name, keys, import_):
    '''Create a BFU within the specified import and associate the given files

    Args:
        uuid (:obj:`str`): UUID of the BFU
        name (:obj:`str`): Name of the BFU
        keys (:obj:`str`): Keys of the associated files, the first entry is
            the entrypoint
        import_ (:obj:`str`): UUID of the import
    '''

    keys = ['<{}>'.format(key) for key in keys]
    entrypoint = keys[0]
    keys = ' '.join(keys)

    return PREFIX + '''
        INSERT {
            ?bfu rdf:type :BFU ;
                 :name "%s" ;
                 :inImport ?import .
            ?file :bfu ?bfu .
            ?entrypoint :entrypoint true .
        } WHERE {
            BIND (d:%s AS ?bfu)
            BIND (d:%s AS ?import)
            BIND (%s AS ?entrypoint)
            ?import rdf:type :Import .
            ?file rdf:type :File ;
                  :inImport ?import .
            VALUES ?file { %s }
            ?entrypoint rdf:type :File ;
                        :inImport ?import .
        }
    ''' % (name, uuid, import_, entrypoint, keys)


def query_bfu_details(uuid):
    '''Query details of the specified BFU

    Args:
        uuid (:obj:`str`): UUID of the BFU
    '''

    return PREFIX + '''
        SELECT ?name ?import
        WHERE {
            d:%s rdf:type :BFU ;
                  :name ?name ;
                  :inImport ?import .
        }
    ''' % uuid


def query_bfu_files(uuid):
    '''Query files in the specified BFU

    Args:
        uuid (:obj:`str`): UUID of the BFU
    '''

    return PREFIX + '''
        SELECT ?key ?entrypoint
        WHERE {
            ?file rdf:type :File ;
                  :key ?key ;
                  :bfu d:%s .
            OPTIONAL { ?file :entrypoint ?entrypoint }
        }
    ''' % uuid


def create_image(uuid, name, key, pyramid_levels, bfu):
    '''Create image within the specified BFU

    Args:
        uuid  (:obj:`str`): UUID of the image
        name (:obj:`str`): Name of the import
        key (:obj:`str`): Prefix key of the image
        pyramid_levels (int): Number of pyramid levels
        bfu (:obj:`str`): UUID of the BFU
    '''

    return PREFIX + '''
        INSERT {
            d:%s rdf:type :Image ;
                 :name "%s" ;
                 :key "%s" ;
                 :pyramidLevels %d ;
                 :inBFU ?bfu .
        } WHERE {
            BIND (d:%s AS ?bfu)
            ?bfu rdf:type :BFU .
        }
    ''' % (uuid, name, key, pyramid_levels, bfu)


def query_image_details(uuid):
    '''Query details of the specified image

    Args:
        uuid (:obj:`str`): UUID of the image
    '''

    return PREFIX + '''
        SELECT ?name ?key ?pyramidLevels ?bfu
        WHERE {
            d:%s rdf:type :Image ;
                  :name ?name ;
                  :key ?key ;
                  :pyramidLevels ?pyramidLevels ;
                  :inBFU ?bfu .
        }
    ''' % uuid


# def queryAllGroupNames():
#
#     statement = PREFIX + '''
#         SELECT ?name
#         WHERE { ?s :name ?name }
#     '''
#
#     printResults(connection.query(statement))
#
#
# def queryGroupMembership():
#
#     statement = PREFIX + '''
#         SELECT ?lab ?name ?labType ?userType
#         WHERE {
#             ?user :memberOf ?lab .
#             ?user :name ?name .
#             ?lab rdf:type ?labType .
#             ?user rdf:type ?userType .
#         }
#     '''
#
#     printResults(connection.query(statement))
#
#
# def queryStructure():
#
#     statement = PREFIX + '''
#         SELECT ?subject
#         WHERE {
#             ?subject rdf:type ?type .
#             ?type rdfs:subClassOf* :Subject .
#         }
#     '''
#
#     printResults(connection.query(statement))


# TODO Is there a way to get the database to return a boolean when the required
# permission is not found? Currently this returns granted=true, but never
# granted=false
# def has_permission(user, resource, permission='Read'):
#
#     return PREFIX + '''
#         SELECT (bound(?subject) as ?granted)
#         WHERE {
#             ?subject :%s d:%s .
#             ?subject rdf:type ?type .
#             ?type rdfs:subClassOf* :Subject .
#             cup:%s :memberOf* ?subject .
#         }
#         LIMIT 1
#     ''' % (permission, resource, user)


# # Get the permissions the subject has on the repository
# def getPermissions(user, repo):
#
#     statement = PREFIX + '''
#         SELECT DISTINCT ?permission
#         WHERE {
#             ?subject ?permission d:%s .
#             ?subject rdf:type ?type .
#             ?type rdfs:subClassOf* :Subject .
#             d:%s :memberOf* ?subject .
#         }
#     ''' % (repo, user)
#
#     printResults(connection.query(statement))
#
#
# # List the BFUs in a repository
# def listBFUs(repo):
#
#     statement = PREFIX + '''
#         SELECT ?bfu
#         WHERE {
#             ?bfu :inRepository d:%s .
#         }
#     ''' % repo
#
#     printResults(connection.query(statement))
#
#
# # Get the primary objects associated with a BFU (including whether the
# # object is the entrypoint)
# def listOriginal(bfu):
#
#     statement = PREFIX + '''
#         SELECT ?key ?entrypoint
#         WHERE {
#             d:%s :original ?originals .
#             ?originals :obj ?bff .
#             ?bff :key ?key .
#             OPTIONAL { ?bff :entrypoint ?entrypoint . }
#         }
#     ''' % bfu
#
#     printResults(connection.query(statement))
#

# def createImport(repo, key):
#
#     s3 = 's3://{}'.format(key)
#
#     statement = PREFIX + '''
#         INSERT DATA {
#
#             d:%s rdf:type :Import ;
#                  :key "%s" ;
#                  :inRepository d:%s .
#         }
#     ''' % (key, s3, repo)
#
#     connection.update(statement)


# def create_import(user, repo, key):
#
#     s3 = 's3://{}'.format(key)
#
#     return PREFIX + '''
#         INSERT {
#
#             d:%s rdf:type :Import ;
#                  :key "%s" ;
#                  :inRepository d:%s .
#         } WHERE {
#             d:%s rdf:type :User ;
#                  :memberOf* ?subject .
#             ?subject :Write d:%s
#
#         }
#     ''' % (key, s3, repo, user, repo)


# def list_imports(repo):
#
#     return PREFIX + '''
#         SELECT ?key (bound(?comp) as ?complete)
#         WHERE {
#             ?import rdf:type :Import ;
#                     :inRepository d:%s ;
#                     :key ?key .
#             OPTIONAL { ?import :complete ?comp . }
#
#         }
#     ''' % repo


# def list_imports_with_auth(user, repo):
#
#     return PREFIX + '''
#         SELECT ?key
#         WHERE {
#
#             ?subject :Read d:%s ;
#                      rdf:type ?type .
#             ?type rdfs:subClassOf+ :Subject .
#             d:%s rdf:type :User ;
#                  :memberOf* ?subject .
#             d:%s rdf:type :Repository .
#             ?import rdf:type :Import ;
#                     :inRepository d:%s ;
#                     :key ?key .
#         }
#     ''' % (repo, user, repo, repo)

# def list_imports_with_auth(user, repo):
#
#     return PREFIX + '''
#         SELECT ?key
#         WHERE {
#             BIND (d:%s AS ?repo)
#             BIND (d:%s AS ?user)
#             ?subject :Read ?repo ;
#                      rdf:type ?type .
#             ?type rdfs:subClassOf+ :Subject .
#             ?user rdf:type :User ;
#                  :memberOf* ?subject .
#             ?repo rdf:type :Repository .
#             ?import rdf:type :Import ;
#                     :inRepository ?repo ;
#                     :key ?key .
#         }
#     ''' % (repo, user)


# def create_import(repo, key):
#
#     s3 = 's3://{}'.format(key)
#
#     return PREFIX + '''
#         INSERT {
#             ?import rdf:type :Import ;
#                     :key ?key ;
#                     :inRepository ?repo .
#         } WHERE {
#             BIND (d:%s AS ?import)
#             BIND ("%s" AS ?key)s
#             BIND (d:%s AS ?repo)
#             MINUS {
#                 ?import rdf:type :Import .
#             }
#         }
#     ''' % (key, s3, repo)


# def listAllImports():
#
#     statement = PREFIX + '''
#         SELECT ?key (bound(?comp) as ?complete)
#         WHERE {
#             ?import rdf:type :Import ;
#                     :key ?key .
#             OPTIONAL { ?import :complete ?comp . }
#         }
#     '''
#
#     printResults(connection.query(statement))
#
#
# def completeImport(key):
#
#     statement = PREFIX + '''
#         INSERT DATA {
#
#             d:%s rdf:type :Import ;
#                  :complete true .
#         }
#     ''' % key
#
#     connection.update(statement)
#
#
# def checkSubjectPermissionOnImport(subject, key, permission='Read'):
#
#     statement = PREFIX + '''
#         SELECT (bound(?repo) as ?granted)
#         WHERE {
#             d:%s :inRepository ?repo .
#             ?subject :%s ?repo ;
#                      rdf:type ?type .
#             ?type rdfs:subClassOf* :Subject .
#             d:%s :memberOf* ?subject .
#         }
#         LIMIT 1
#
#     ''' % (key, permission, subject)
#
#     printResults(connection.query(statement))
#
#
