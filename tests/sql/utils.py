from src.minerva_db.sql.api.utils import to_jsonapi


class TestUtils():

    def test_to_jsonapi(self):
        data = {
            'x': 1,
            'y': 'foo'
        }
        included = {}
        assert to_jsonapi(data) == {
            'data': data,
            'included': included
        }

    def test_to_jsonapi_included(self):
        data = {
            'x': 1,
            'y': 'foo'
        }
        included = {
            'extras': [{
                'a': 'A'
            }]
        }
        assert to_jsonapi(data, included) == {
            'data': data,
            'included': included
        }
