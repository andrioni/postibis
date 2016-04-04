import sqlalchemy as sa

from ibis.client import Database
from .compiler import PostgresDialect, postgres_schema_from_table
from ibis.compat import lzip
import ibis.expr.operations as ops
import ibis.expr.types as ir
import ibis.sql.alchemy as alch
import ibis.common as com


class PostgresTable(alch.AlchemyTable):
    def __init__(self, table, source):
        self.sqla_table = table

        schema = postgres_schema_from_table(table)
        name = table.name

        ops.TableNode.__init__(self, [name, schema, source])
        ops.HasSchema.__init__(self, schema, name=name)


class PostgresDatabase(Database):
    pass


class PostgresClient(alch.AlchemyClient):

    """
    The Ibis Postgres client class
    """

    dialect = PostgresDialect
    database_class = PostgresDatabase

    def __init__(self, path):
        url = sa.engine.url.make_url(path)
        assert url.get_backend_name() == 'postgresql'
        self.name = url.database
        # TODO: Document this behavior
        # Postgres considers the database to be something separate from the schema
        self.database_name = "public"
        self.con = sa.create_engine(url)
        self.meta = sa.MetaData(bind=self.con)

    @property
    def current_database(self):
        return self.database_name

    def list_databases(self, like=None):
        """
        List PostgreSQL schemas using the standard information_schema.schemata
        table.

        Parameters
        ----------
        like : string, default None
          e.g. 'foo%' to match all tables starting with 'foo'

        Returns
        -------
        databases : list of strings
        """
        statement = 'SELECT schema_name FROM information_schema.schemata'
        if like:
            # Automatically escape %s, otherwise SQLAlchemy complains
            statement += " WHERE schema_name LIKE '{0}'".format(like.replace('%', '%%'))

        with self._execute(statement, results=True) as cur:
            results = self._get_list(cur)

        return results

    def set_database(self):
        raise NotImplementedError

    @property
    def client(self):
        return self

    def table(self, name, database=None):
        """
        Create a table expression that references a particular table in the
        Postgres database

        Parameters
        ----------
        name : string

        Returns
        -------
        table : TableExpr
        """
        alch_table = self._get_sqla_table(name)
        node = PostgresTable(alch_table, self)
        return self._table_expr_klass(node)

    def drop_table(self):
        pass

    def create_table(self, name, expr=None, schema=None, database=None):
        pass

    @property
    def _table_expr_klass(self):
        return ir.TableExpr

    def _get_list(self, cur):
        tuples = cur.fetchall()
        if len(tuples) > 0:
            return list(lzip(*tuples)[0])
        else:
            return []
