def assert_rowsets_equal(a, b):

    assert len(a) == len(b)

    if len(a) == 0:
        return

    def kfunc(row):
        # Get the keys used in the row, ordered
        keys = sorted(row.keys())

        # Yield tuples with the keys and their value
        return [(key, row[key]) for key in keys]

    a = sorted(a, key=kfunc)
    b = sorted(b, key=kfunc)

    assert a == b
