package de.ag.sqala.sql

import java.io.{StringWriter, Writer}
import de.ag.sqala.StringUtils._
import de.ag.sqala.{OrderDirection, Descending, Ascending}
import de.ag.sqala.relational.Schema

/**
 *
 */
sealed abstract class Query {

  protected def writeAttributes(out: Writer, param: WriteParameterization, attributes: Seq[QuerySelectAttribute]) {
    if (attributes.isEmpty) {
      out.write("*")
    } else {
      writeJoined[QuerySelectAttribute](out, attributes, ", ", {
        (out, attr) => attr.expr match {
          case ExprColumn(alias) if alias == attr.alias =>
            out.write(alias)
          case expr =>
            expr.write(out, param)
            writeAlias(out, attr.alias)
        }
      })
    }
  }

  /**
   * Writes " AS " + alias if alias is set
   * @param out        output sink
   * @param maybeAlias alias to write, if any
   */
  protected def writeAlias(out: Writer, maybeAlias: Option[String]) {
    maybeAlias match {
      case None =>
      case Some(alias) => out.write(" AS "); out.write(alias)
    }
  }

  protected def writeFrom(out: Writer, param: WriteParameterization, from: Seq[QuerySelectFromQuery]) {
    out.write("FROM ")
    writeJoined[QuerySelectFromQuery](out, from, ", ", {
      (out, from) => from.query match {
        case q: QueryTable => out.write(q.name)
        case q =>
          out.write("(")
          q.write(out, param)
          out.write(")")
      }
        writeAlias(out, from.alias)
    })
  }

  protected def writeWhere(out: Writer, param: WriteParameterization, where: Seq[Expr]) {
    out.write("WHERE ")
    writeJoined[Expr](out, where, " AND ", {
      (out, expr) => expr.write(out, param)
    })
  }

  protected def writeGroupBy(out: Writer, param: WriteParameterization, groupBys: Seq[Expr]) {
    out.write("GROUP BY ")
    writeJoined[Expr](out, groupBys, ", ", {
      (out, groupBy) => groupBy.write(out, param)
    })
  }

  protected def writeHaving(out: Writer, param: WriteParameterization, having: Expr) {
    out.write("HAVING ")
    having.write(out, param)
  }

  protected def writeOrderBy(out: Writer, param: WriteParameterization, orderBys: Seq[QuerySelectOrderBy]) {
    out.write("ORDER BY ")
    writeJoined[QuerySelectOrderBy](out, orderBys, ", ", {
      (out, orderBy) =>
        orderBy.expr.write(out, param)
        out.write(orderBy.order match {
          case Ascending => " ASC"
          case Descending => " DESC"
        })
    })
  }

  def write(out:Writer, param:WriteParameterization) {
    this match {
      case QueryTable(name) =>
        out.write("SELECT * FROM ")
        out.write(name)
      case s:QuerySelect =>
        out.write("SELECT")
        writeWithSpaceIfNotEmpty(out, s.options)(writeJoined(out, _, " "))
        writeSpace(out); writeAttributes(out, param, s.attributes)
        writeWithSpaceIfNotEmpty(out, s.from)(writeFrom(out, param, _))
        writeWithSpaceIfNotEmpty(out, s.where)(writeWhere(out, param, _))
        writeWithSpaceIfNotEmpty(out, s.groupBy)(writeGroupBy(out, param, _))
        if (s.having.isDefined) {
          writeSpace(out)
          writeHaving(out, param, s.having.get)
        }
        writeWithSpaceIfNotEmpty(out, s.orderBy)(writeOrderBy(out, param, _))
        writeWithSpaceIfNotEmpty(out, s.extra)(writeJoined(out, _, " "))
      case s:QueryCombine =>
        param.writeCombine(out, param, s)
      case QueryEmpty =>
    }
  }

  def toString(param:WriteParameterization) = {
    val result = new StringWriter()
    write(result, param)
    result.toString
  }

  override def toString = toString(defaultSqlWriteParameterization)
}

case class QueryTable(name: Query.TableName) extends Query

case class QuerySelect(options: Seq[String], // DISTINCT, ALL, etc.
                       attributes: Seq[QuerySelectAttribute], // selected fields (expr + alias), empty seq for '*'
                       //                     isNullary: Boolean, // true if select represents nullary relation (?); in this case attributes should contain single dummy attribute (?)
                       from: Seq[QuerySelectFromQuery], // FROM (
                       where: Seq[Expr], // WHERE; Seq constructed from relational algebra
                       groupBy: Seq[Expr], // GROUP BY
                       having: Option[Expr], // HAVING
                       orderBy: Seq[QuerySelectOrderBy], // ORDER BY
                       extra: Seq[String] // TOP n, etc.
                        ) extends Query

case class QueryCombine(op: CombineOp,
                        left: Query,
                        right: Query) extends Query

case object QueryEmpty extends Query // FIXME used when?

case class QuerySelectAttribute(expr:Expr, alias:Option[Query.ColumnName])
case class QuerySelectFromQuery(query:Query, alias:Option[Query.TableName])
case class QuerySelectOrderBy(expr:Expr, order:OrderDirection)

object Query {
  type ColumnName = String

  type TableName = String

  case class Table(name: TableName,
                   schema: Schema)

  def defaultWriteCombine(out:Writer, param:WriteParameterization, Combine:QueryCombine) {
    out.write('(')
    Combine.left.write(out, param)
    out.write(") ")
    out.write(Combine.op match {
      case CombineOpUnion => "UNION"
      case CombineOpIntersect => "INTERSECT"
      case CombineOpExcept => "EXCEPT"
    })
    out.write(" (")
    Combine.right.write(out, param)
    out.write(")")
  }

  def defaultWriteLiteral(out:Writer, literal:Literal) {
    literal match {
      case LiteralBoolean(b) => out.write(if (b) "TRUE" else "FALSE")
      case LiteralNull => out.write("NULL")
      case LiteralInteger(n) => out.write(n.toString)
      case LiteralDouble(d) => out.write(d.toString)
      case LiteralDecimal(d) => out.write(d.toString())
      case LiteralString(s) => out.write('\'')
        for(c <- s) {
          if (c == '\'') out.write('\'')
          out.write(c)
        }
        out.write('\'')
    }
  }

  def makeSelect(options:Seq[String]=Seq(),
                 attributes:Seq[QuerySelectAttribute]=Seq(),
                 from:Seq[QuerySelectFromQuery],
                 where: Seq[Expr]=Seq(),
                 groupBy: Seq[Expr]=Seq(),
                 having: Option[Expr]=None,
                 orderBy: Seq[QuerySelectOrderBy]=Seq(),
                 extra:Seq[String]=Seq()) = {
    QuerySelect(options, attributes, from, where, groupBy, having, orderBy, extra)
  }
}