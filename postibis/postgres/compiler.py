import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pst

from ibis.sql.alchemy import unary, varargs, fixed_arity
import ibis.sql.alchemy as alch
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.types as ir

import postibis.common.compiler as com


def _group_concat(t, expr):
    col, sep = expr.op().args
    sa_col = t.translate(col)
    sa_sep = t.translate(sep)
    # Should support Postgres 8.4+, but not Redshift
    return sa.func.array_to_string(sa.func.array_agg(sa_col), sa_sep)

_operation_registry = com.operation_registry.copy()

_operation_registry.update({
    ops.GroupConcat: _group_concat
})

_postgres_type_to_ibis = com.type_to_ibis.copy()

_postgres_type_to_ibis.update({
    pst.DATE: dt.Timestamp,
    pst.JSON: dt.String,
    pst.JSONB: dt.String,
    pst.TIMESTAMP: dt.Timestamp,
})



def postgres_schema_from_table(table):
    # Convert SQLA table to Ibis schema
    names = table.columns.keys()

    types = []
    for c in table.columns.values():
        type_class = type(c.type)

        if isinstance(c.type, sa.types.NUMERIC):
            t = dt.Decimal(c.type.precision,
                           c.type.scale,
                           nullable=c.nullable)
        else:
            if c.type in _postgres_type_to_ibis:
                ibis_class = _postgres_type_to_ibis[c.type]
            elif type_class in _postgres_type_to_ibis:
                ibis_class = _postgres_type_to_ibis[type_class]
            else:
                raise NotImplementedError(c.type)
            t = ibis_class(c.nullable)

        types.append(t)

    return dt.Schema(names, types)


class PostgresExprTranslator(alch.AlchemyExprTranslator):

    _registry = _operation_registry
    _rewrites = alch.AlchemyExprTranslator._rewrites.copy()
    _type_map = alch.AlchemyExprTranslator._type_map.copy()
    # _type_map.update({
    #     dt.Double: sa.types.REAL,
    #     dt.Float: sa.types.REAL
    # })

class PostgresDialect(alch.AlchemyDialect):

    translator = PostgresExprTranslator
