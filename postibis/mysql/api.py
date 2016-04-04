from .client import MySQLClient

def compile(expr):
    """
    Force compilation of expression for the MySQL target
    """
    from .client import MySQLDialect
    from ibis.sql.alchemy import to_sqlalchemy
    return to_sqlalchemy(expr, dialect=MySQLDialect)


def connect(path=None):

    """
    Create an Ibis client connected to a MySQL database.

    Multiple database files can be created using the attach() method

    Parameters
    ----------
    path : string, default None
        File path to the SQLite database file. If None, creates an in-memory
        transient database and you can use attach() to add more files
    """

    return MySQLClient(path)
