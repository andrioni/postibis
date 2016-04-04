import sqlalchemy as sa
import sqlalchemy.dialects.mysql as msql

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
    return sa.func.group_concat(sa_col.op("SEPARATOR")(sa_sep))

_operation_registry = com.operation_registry.copy()

_operation_registry.update({
    ops.GroupConcat: _group_concat
})


def _enum(rep_type, value_type):
    def enum(nullable=True):
        return dt.Enum(rep_type, value_type, nullable)
    return enum

_mysql_type_to_ibis = com.type_to_ibis.copy()

_mysql_type_to_ibis.update({
    msql.BIGINT: dt.Int64,
    msql.VARCHAR: dt.String,
    msql.TEXT: dt.String,
    msql.DATE: dt.Timestamp,
    msql.DATETIME: dt.Timestamp,
    msql.TIMESTAMP: dt.Timestamp,
    msql.DECIMAL: dt.Decimal,
    msql.ENUM: _enum(dt.String, dt.String)
})


def mysql_schema_from_table(table):
    # Convert SQLA table to Ibis schema
    names = table.columns.keys()

    types = []
    for c in table.columns.values():
        type_class = type(c.type)

        # TODO: Find a new (and reasonable) way to do this dispatch
        if (isinstance(c.type, sa.types.NUMERIC) or
            isinstance(c.type, msql.DECIMAL)):
            t = dt.Decimal(c.type.precision,
                           c.type.scale,
                           nullable=c.nullable)
        else:
            if c.type in _mysql_type_to_ibis:
                ibis_class = _mysql_type_to_ibis[c.type]
            elif type_class in _mysql_type_to_ibis:
                ibis_class = _mysql_type_to_ibis[type_class]
            else:
                print type_class
                print _mysql_type_to_ibis
                raise NotImplementedError(c.type)
            t = ibis_class(c.nullable)

        types.append(t)

    return dt.Schema(names, types)


class MySQLExprTranslator(alch.AlchemyExprTranslator):

    _registry = _operation_registry
    _rewrites = alch.AlchemyExprTranslator._rewrites.copy()
    _type_map = alch.AlchemyExprTranslator._type_map.copy()
    # _type_map.update({
    #     dt.Double: sa.types.REAL,
    #     dt.Float: sa.types.REAL
    # })

class MySQLDialect(alch.AlchemyDialect):

    translator = MySQLExprTranslator
