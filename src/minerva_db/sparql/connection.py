from SPARQLWrapper import SPARQLWrapper, JSON


class Connection:

    def __init__(self, endpoint):
        self.endpoint = endpoint

    def execute(self, statement: str, update: bool = False):

        sparql = SPARQLWrapper(self.endpoint, self.endpoint + '/statements')
        sparql.setQuery(statement)
        sparql.setReturnFormat(JSON)

        if update:
            sparql.setMethod('POST')
            sparql.query()
            return

        else:
            return sparql.query().convert()

    def query(self, statement: str):

        results = self.execute(statement)

        header = results['head']['vars']
        data = [
            {
                k: v['value']
                for (k, v) in result.items()
            }
            for result in results['results']['bindings']
        ]

        return (header, data)

    def update(self, statement: str):
        return self.execute(statement, True)

    def ask(self, statement: str) -> bool:
        '''Special case query for ASK

        Args:
            statement: Query

        Returns:

        '''

        return self.execute(statement)['boolean']
