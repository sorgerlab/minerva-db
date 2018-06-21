from minerva_db.sparql import Client
import os
import pkg_resources
from terminaltables import AsciiTable

endpoint = 'http://localhost:8080/rdf4j-server/repositories/test'
prefix = pkg_resources.resource_string(
    'minerva_db.sparql',
    os.path.join('schema', 'prefix.rq')
).decode('utf-8')


def print_results(results, client=False):

    if client is True:
        data = results
        if len(results) > 0:
            header = results[0].keys()
        else:
            header = []
    else:
        header, data = results

    table_data = [[d.get(h) for h in header] for d in data]

    print(AsciiTable([header] + table_data).table)


client = Client(endpoint)
client._init_db()
conn = client._connection()

client.create_user('bob', 'Bob', 'bob@example.com')
client.create_user('bob', 'Bill', 'bill@example.com')

results = conn.query(prefix + '''
    SELECT ?user ?name ?email
    WHERE {
        BIND (cup:bob AS ?user)
        ?user rdf:type :User ;
              :name ?name ;
              :email ?email .
    }
''')

print_results(results)

# conn.execute('''
#     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#     PREFIX : <http://schema#>
#     PREFIX d: <http://data#>
#     INSERT DATA {
#         d:a rdf:type :User ;
#             :name "bob" ;
#             :val 1 .
#         d:b rdf:type :User ;
#             :name "bill" ;
#             :val 1 .
#         d:d rdf:type :User ;
#             :name "ben" ;
#             :val 2 .
#     }
# ''')
#
# # results = conn.query(prefix + '''
# #     SELECT ?user (SAMPLE(DISTINCT ?name) AS ?names)
# #     WHERE {
# #         ?user rdf:type :User ;
# #               :name ?name .
# #     }
# #     GROUP BY ?user
# # ''')
#
# results = conn.query('''
#     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#     PREFIX : <http://schema#>
#     PREFIX d: <http://data#>
#     SELECT ?val (GROUP_CONCAT(DISTINCT ?name) AS ?names)
#     WHERE {
#         ?user rdf:type :User ;
#               :name ?name ;
#               :val ?val .
#     }
#     GROUP BY ?val
# ''')
#
# print_results(results)
#
# results = conn.query('''
#     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#     PREFIX : <http://schema#>
#     PREFIX d: <http://data#>
#     SELECT ?val (GROUP_CONCAT(DISTINCT ?name) AS ?names)
#     WHERE {
#         BIND (3 AS ?val)
#         ?user rdf:type :User ;
#               :name ?name ;
#               :val ?val .
#     }
#     GROUP BY ?val
# ''')
#
# print_results(results)
#
# results = conn.query('''
#     PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
#     PREFIX : <http://schema#>
#     PREFIX d: <http://data#>
#     SELECT ?val (GROUP_CONCAT(DISTINCT ?name) AS ?names)
#     WHERE {
#         BIND (3 AS ?val)
#         ?user rdf:type :User ;
#               :name ?name ;
#               :val ?val .
#     }
#     GROUP BY ?val
#     HAVING(COUNT(?val) > 0)
# ''')
#
# print_results(results)

# client.create_user('bob', 'Bob', 'bob@example.com')
# client.create_user('bill', 'Bill', 'bill@example.com')
# client.create_user('jane', 'Jane', 'jane@example.com')
# client.create_group('lab1', 'Lab One', 'bill')
# client.create_repository('repo1', 'Repo One', 'bob')
# # client.add_user_to_repository('repo1', 'bill', ['Read'])
# client.add_group_to_repository('repo1', 'lab1', ['Read'])
#
# statement = prefix + '''
#     SELECT (?subject AS ?uuid)
#            ?subjectType
#            ?name
#            (group_concat(DISTINCT ?permission) AS ?permissions)
#     WHERE {
#         BIND(d:%s AS ?repository)
#         ?subjectType rdfs:subClassOf :Subject .
#         ?subject rdf:type ?subjectType ;
#                  ?permission ?repository ;
#                  :name ?name .
#         ?permission rdfs:subPropertyOf :Permission .
#         ?repository rdf:type :Repository .
#         ?subject ?permission ?repository .
#     }
#     GROUP BY ?subject ?subjectType ?name
#     HAVING (bound(?subject))
# ''' % 'repo1'
#
# results = conn.query(statement)
# print_results(results)
# results = client.list_repository_subjects('repo1')
# print_results(results, True)
