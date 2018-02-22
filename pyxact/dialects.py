
class SQLDialect:
    pass

class sqliteDialect(SQLDialect):
    placeholder = '?' # The placeholder to use for parametised queries
    native_decimals = False # Whether the adaptor supports decimals natively
                            # or whether they must be converted to strings
