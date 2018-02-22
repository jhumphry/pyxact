
class SQLDialect:
    pass

class sqliteDialect(SQLDialect):
    placeholder = '?' # The placeholder to use for parametised queries
    native_decimals = False # Whether the adaptor supports decimals natively
                            # otherwise they must be converted to strings
    native_booleans = False # Whether the adaptor supports booleans, otherwise
                            # they must be converted to 0/1 integers
