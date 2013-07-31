package de.ag.sqala.sql

import java.io.{StringWriter, Writer}
import de.ag.sqala.StringUtils._
import de.ag.sqala.{sql, OrderDirection, Descending, Ascending}
import de.ag.sqala.relational.Schema

/**
 * SQL view
 */
sealed abstract class View {

  /**
   * Write comma-seperated list of attributes to output sink, or '*' if attributes is empty.
   *
   * Usually writes "expr AS alias", unless expr == alias, in which case it just writes "alias".
   * @param out        output sink
   * @param param      write parameterization
   * @param attributes attributes to write
   */
  protected def writeAttributes(out: Writer, param: WriteParameterization, attributes: Seq[View.SelectAttribute]) {
    if (attributes.isEmpty) {
      out.write("*")
    } else {
      writeJoined[View.SelectAttribute](out, attributes, ", ", {
        (out, attr) => attr.expr match {
          case Expr.Column(alias) if alias == attr.alias.getOrElse(alias) =>
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

  /**
   * Write FROM queries, separated by comma.
   *
   * Eg., "FROM A, (B UNION C)"
   * @param out   output sink
   * @param param write parameterization
   * @param from  FROM queries to write
   */
  protected def writeFrom(out: Writer, param: WriteParameterization, from: Seq[View.SelectFromView]) {
    out.write("FROM ")
    writeJoined[View.SelectFromView](out, from, ", ", {
      (out, from) => from.view match {
        case View.Table(name, _) => out.write(name)
        case q =>
          out.write("(")
          q.write(out, param)
          out.write(")")
      }
        writeAlias(out, from.alias)
    })
  }

  /**
   * Write WHERE clause, separated by "AND".
   *
   * Eg., "WHERE (id < 12) AND (id > 2)"
   * @param out    output sink
   * @param param  write parameterization
   * @param where  where clauses
   */
  protected def writeWhere(out: Writer, param: WriteParameterization, where: Seq[Expr]) {
    out.write("WHERE ")
    writeJoined[Expr](out, where, " AND ", {
      (out, expr) => expr.write(out, param)
    })
  }

  /**
   * Write GROUP BY clauses, separated by comman.
   *
   * Eg. "GROUP BY dept, employee"
   * @param out       output sink
   * @param param     write parameterization
   * @param groupBys  group-by clauses
   */
  protected def writeGroupBy(out: Writer, param: WriteParameterization, groupBys: Seq[Expr]) {
    out.write("GROUP BY ")
    writeJoined[Expr](out, groupBys, ", ", {
      (out, groupBy) => groupBy.write(out, param)
    })
  }

  /**
   * Write HAVING clause as-is
   * @param out     output sink
   * @param param   write parameterization
   * @param having  having clause
   */
  protected def writeHaving(out: Writer, param: WriteParameterization, having: Expr) {
    out.write("HAVING ")
    having.write(out, param)
  }

  /**
   * Write ORDER BY clause, separated by comma
   *
   * Eg., "ORDER BY size ASC, weight DESC"
   * @param out      output sink
   * @param param    write paramaterization
   * @param orderBys order-by clauses
   */
  protected def writeOrderBy(out: Writer, param: WriteParameterization, orderBys: Seq[View.SelectOrderBy]) {
    out.write("ORDER BY ")
    writeJoined[View.SelectOrderBy](out, orderBys, ", ", {
      (out, orderBy) =>
        orderBy.expr.write(out, param)
        out.write(orderBy.order match {
          case Ascending => " ASC"
          case Descending => " DESC"
        })
    })
  }

  /**
   * Write view to output sink.
   *
   * Eg., "SELECT id, name FROM persons WHERE (id > 12) ORDER BY id DESC"
   * @param out   output sink
   * @param param write parameterization
   */
  def write(out:Writer, param:WriteParameterization) {
    this match {
      case View.Table(name, _) =>
        out.write("SELECT * FROM ")
        out.write(name)
      case s:View.Select =>
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
      case s:View.Combine =>
        param.writeCombine(out, param, s)
      case View.Empty =>
    }
  }

  /**
   * Turn view to string
   * @param param write parameterization
   * @return      SQL string of this view
   */
  def toString(param:WriteParameterization) = {
    val result = new StringWriter()
    write(result, param)
    result.toString
  }

  /**
   * Turn view to string using default write parameterization
   * @return SQL string of this view
   */
  override def toString = toString(defaultSqlWriteParameterization)

  /**
   * Turn this View to a View.Select
   * @return a View.Select that is semantically equivalent to this View
   */
  def toSelect: sql.View.Select = {
    this match {
      case sql.View.Empty => sql.View.makeSelect(from=Seq())
      case select:sql.View.Select if select.attributes.isEmpty => select
      case _ => sql.View.makeSelect(from=Seq(sql.View.SelectFromView(this, None)))
    }
  }
}

object View {
  type ColumnName = String

  type TableName = String

  /** plain ref to table */
  case class Table(name: String, schema: Schema) extends View {
    /** convert view to a relational query */
    def toQuery: de.ag.sqala.relational.Query.Base = de.ag.sqala.relational.Query.Base(name, schema)
  }

  /** select from with all clauses + options + extra */
  case class Select(options: Seq[String], // DISTINCT, ALL, etc.
                         attributes: Seq[SelectAttribute], // selected fields (expr + alias), empty seq for '*'
                         //                     isNullary: Boolean, // true if select represents nullary relation (?); in this case attributes should contain single dummy attribute (?)
                         from: Seq[SelectFromView], // FROM (
                         where: Seq[Expr], // WHERE; Seq constructed from relational algebra
                         groupBy: Seq[Expr], // GROUP BY
                         having: Option[Expr], // HAVING
                         orderBy: Seq[SelectOrderBy], // ORDER BY
                         extra: Seq[String] // TOP n, etc.
                          ) extends View

  /** combine two queries */
  case class Combine(op: Expr.CombineOp,
                          left: View,
                          right: View) extends View

  /** the empty view */
  case object Empty extends View // FIXME used when?

  /** select-from attributes with optional alias */
  case class SelectAttribute(expr:Expr, alias:Option[View.ColumnName])
  /** select-from table (FROM clause) with optional alias */
  case class SelectFromView(view:View, alias:Option[View.TableName])
  /** select-from order-by clause with order direction */
  case class SelectOrderBy(expr:Expr, order:OrderDirection)

  /**
   * Helper method with defaults to create View.Select
   * @param options     options (DISTINCT, etc.), defaults to Seq()
   * @param attributes  attributes (columns, expressions, etc.), defaults to Seq() ("*")
   * @param from        from clauses, required
   * @param where       where clauses, defaults to Seq()
   * @param groupBy     group-by clauses, defaults to Seq()
   * @param having      having clause, defaults to None
   * @param orderBy     order-by clauses, defaults to Seq()
   * @param extra       extra text (eg. "LIMIT 2"), defaults to Seq()
   * @return            constructed View.Select
   */
  def makeSelect(options:Seq[String]=Seq(),
                 attributes:Seq[SelectAttribute]=Seq(),
                 from:Seq[SelectFromView],
                 where: Seq[Expr]=Seq(),
                 groupBy: Seq[Expr]=Seq(),
                 having: Option[Expr]=None,
                 orderBy: Seq[SelectOrderBy]=Seq(),
                 extra:Seq[String]=Seq()) = {
    Select(options, attributes, from, where, groupBy, having, orderBy, extra)
  }
}