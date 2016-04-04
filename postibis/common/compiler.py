import sqlalchemy as sa

import ibis.expr.types as ir
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.sql.alchemy as alch
from ibis.sql.alchemy import unary, varargs, fixed_arity

type_to_ibis = {
    # Default Ibis conversions
    sa.types.SmallInteger: dt.Int16,
    sa.types.INTEGER: dt.Int64,
    sa.types.BOOLEAN: dt.Boolean,
    sa.types.BIGINT: dt.Int64,
    sa.types.FLOAT: dt.Double,
    sa.types.REAL: dt.Double,
    sa.types.DECIMAL: dt.Decimal,

    sa.types.VARCHAR: dt.String,
    sa.types.TEXT: dt.String,
    sa.types.NullType: dt.String,
    sa.types.Text: dt.String,
}


def _negate(t, expr):
    arg, = expr.op().args
    sa_arg = t.translate(arg)
    if isinstance(expr, ir.BooleanValue):
        return sa.not_(sa_arg)
    else:
        return -sa_arg


def _now(t, expr):
    return sa.func.now()


def _extract(period):
    def extractor(t, expr):
        arg, = expr.op().args
        sa_arg = t.translate(arg)
        return sa.cast(sa.func.extract(period, sa_arg), sa.INTEGER)
    return extractor

operation_registry = alch._operation_registry.copy()

operation_registry.update({
    ops.Negate: _negate,

    ops.ExtractYear: _extract("YEAR"),
    ops.ExtractMonth: _extract("MONTH"),
    ops.ExtractDay: _extract("DAY"),
    ops.ExtractHour: _extract("HOUR"),
    ops.ExtractMinute: _extract("MINUTE"),
    ops.ExtractSecond: _extract("SECOND"),
    ops.ExtractMillisecond: _extract("MILLISECOND"),
    ops.TimestampNow: _now,

    ops.Uppercase: unary("UPPER"),
    ops.Lowercase: unary("LOWER"),

    ops.Strip: unary('TRIM'),
    ops.LStrip: unary('LTRIM'),
    ops.RStrip: unary('RTRIM')
})
