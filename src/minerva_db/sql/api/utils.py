def to_jsonapi(data, included={}):
    return {
        'data': data,
        'included': included
    }
