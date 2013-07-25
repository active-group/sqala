package de.ag.sqala.sql

import java.io.{StringWriter, Writer}
import de.ag.sqala.StringUtils._
import de.ag.sqala.{OrderDirection, Descending, Ascending}
import de.ag.sqala.relational.Schema

/**
 *
 */
sealed abstract class Query {

  protected def writeAttributes(out: Writer, param: WriteParameterization, attributes: Seq[Query.SelectAttribute]) {
    if (attributes.isEmpty) {
      out.write("*")
    } else {
      writeJoined[Query.SelectAttribute](out, attributes, ", ", {
        (out, attr) => attr.expr match {
          case Expr.Column(alias) if alias == attr.alias =>
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

  protected def writeFrom(out: Writer, param: WriteParameterization, from: Seq[Query.SelectFromQuery]) {
    out.write("FROM ")
    writeJoined[Query.SelectFromQuery](out, from, ", ", {
      (out, from) => from.query match {
        case q: Query.Table => out.write(q.name)
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

  protected def writeOrderBy(out: Writer, param: WriteParameterization, orderBys: Seq[Query.SelectOrderBy]) {
    out.write("ORDER BY ")
    writeJoined[Query.SelectOrderBy](out, orderBys, ", ", {
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
      case Query.Table(name) =>
        out.write("SELECT * FROM ")
        out.write(name)
      case s:Query.Select =>
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
      case s:Query.Combine =>
        param.writeCombine(out, param, s)
      case Query.Empty =>
    }
  }

  def toString(param:WriteParameterization) = {
    val result = new StringWriter()
    write(result, param)
    result.toString
  }

  override def toString = toString(defaultSqlWriteParameterization)
}

object Query {
  type ColumnName = String

  type TableName = String

  case class Table(name: Query.TableName) extends Query

  case class Select(options: Seq[String], // DISTINCT, ALL, etc.
                         attributes: Seq[SelectAttribute], // selected fields (expr + alias), empty seq for '*'
                         //                     isNullary: Boolean, // true if select represents nullary relation (?); in this case attributes should contain single dummy attribute (?)
                         from: Seq[SelectFromQuery], // FROM (
                         where: Seq[Expr], // WHERE; Seq constructed from relational algebra
                         groupBy: Seq[Expr], // GROUP BY
                         having: Option[Expr], // HAVING
                         orderBy: Seq[SelectOrderBy], // ORDER BY
                         extra: Seq[String] // TOP n, etc.
                          ) extends Query

  case class Combine(op: Expr.CombineOp,
                          left: Query,
                          right: Query) extends Query

  case object Empty extends Query // FIXME used when?

  case class SelectAttribute(expr:Expr, alias:Option[Query.ColumnName])
  case class SelectFromQuery(query:Query, alias:Option[Query.TableName])
  case class SelectOrderBy(expr:Expr, order:OrderDirection)

  def defaultWriteCombine(out:Writer, param:WriteParameterization, Combine:Combine) {
    out.write('(')
    Combine.left.write(out, param)
    out.write(") ")
    out.write(Combine.op match {
      case Expr.CombineOp.Union => "UNION"
      case Expr.CombineOp.Intersect => "INTERSECT"
      case Expr.CombineOp.Except => "EXCEPT"
    })
    out.write(" (")
    Combine.right.write(out, param)
    out.write(")")
  }

  def defaultWriteLiteral(out:Writer, literal:Expr.Literal) {
    literal match {
      case Expr.Literal.Boolean(b) => out.write(if (b) "TRUE" else "FALSE")
      case Expr.Literal.Null => out.write("NULL")
      case Expr.Literal.Integer(n) => out.write(n.toString)
      case Expr.Literal.Double(d) => out.write(d.toString)
      case Expr.Literal.Decimal(d) => out.write(d.toString())
      case Expr.Literal.String(s) => out.write('\'')
        for(c <- s) {
          if (c == '\'') out.write('\'')
          out.write(c)
        }
        out.write('\'')
    }
  }

  def makeSelect(options:Seq[String]=Seq(),
                 attributes:Seq[SelectAttribute]=Seq(),
                 from:Seq[SelectFromQuery],
                 where: Seq[Expr]=Seq(),
                 groupBy: Seq[Expr]=Seq(),
                 having: Option[Expr]=None,
                 orderBy: Seq[SelectOrderBy]=Seq(),
                 extra:Seq[String]=Seq()) = {
    Select(options, attributes, from, where, groupBy, having, orderBy, extra)
  }
}