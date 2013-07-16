package de.ag.sqala

import java.io.{StringWriter, Writer}
import de.ag.sqala.StringUtils._

class Label(val label:String) // FIXME type alias?

class Domain(val typ: String)  // FIXME structured?

case class SqlExprCaseBranch(condition: SqlExpr, expr: SqlExpr)


sealed abstract class SqlExpr {
  def write(out:Writer, param:SqlWriteParameterization) {
    this match {
      case SqlExprConst(value) =>
        param.writeLiteral(out, value)
      case SqlExprTuple(exprs) =>
        out.write("(")
        writeJoined[SqlExpr](out, exprs, ", ", {
          (out, expr) => expr.write(out, param)})
      case SqlExprColumn(columnName) =>
        out.write(columnName)
      case SqlExprApp(operator, operands) =>
        operator match {
          case SqlPostfixOperator(opName) =>
            out.write("(")
            operands.head.write(out, param)
            writeSpace(out)
            out.write(opName)
          case SqlPrefixOperator(opName) =>
            out.write(opName)
            out.write("(")
            operands.head.write(out, param)
            out.write(")")
          case SqlInfixOperator(opName) =>
            out.write("(")
            operands.head.write(out, param)
            writeSpace(out)
            out.write(opName)
            writeSpace(out)
            operands.tail.head.write(out, param)
            out.write(")")
        }
      case SqlExprCase(branches, default) =>
        out.write("(CASE ")
        branches.foreach(writeBranch(out, param, _))
        default match {
          case None =>
          case Some(expr) =>
            out.write(" ELSE ")
            expr.write(out, param)
        }
        out.write(")")
      case SqlExprExists(query) =>
        out.write("EXISTS (")
        query.write(out, param)
        out.write(")")
      case SqlExprSubQuery(query) =>
        out.write("(")
        query.write(out, param)
        out.write(")")
    }
  }

  protected def writeBranch(out:Writer, param:SqlWriteParameterization, branch:SqlExprCaseBranch) {
    out.write("WHEN ")
    branch.condition.write(out, param)
    out.write(" THEN ")
    branch.expr.write(out, param)
  }

  def toString(param:SqlWriteParameterization) = {
    val result = new StringWriter()
    write(result, param)
    result.toString
  }
}

case class SqlExprConst(value: SqlLiteral) extends SqlExpr

case class SqlExprTuple(exprs: Seq[SqlExpr]) extends SqlExpr

case class SqlExprColumn(name: SqlColumnName) extends SqlExpr

case class SqlExprApp(operator: SqlOperator, operands: Seq[SqlExpr]) extends SqlExpr

case class SqlExprCase(branches: Seq[SqlExprCaseBranch], default: Option[SqlExpr]) extends SqlExpr

case class SqlExprExists(query: SqlQuery) extends SqlExpr

case class SqlExprSubQuery(query: SqlQuery) extends SqlExpr

sealed abstract class SqlLiteral
case class SqlLiteralNumber(n:BigDecimal) extends SqlLiteral // FIXME other type than BigDecimal?
case class SqlLiteralString(s:String) extends SqlLiteral
case object SqlLiteralNull extends SqlLiteral
case class SqlLiteralBoolean(b:Boolean) extends SqlLiteral

sealed abstract class SqlOperator(val name: String, val arity: Int)

case class SqlPrefixOperator(override val name: String) extends SqlOperator(name, 1)

case class SqlInfixOperator(override val name: String) extends SqlOperator(name, 2)

case class SqlPostfixOperator(override val name: String) extends SqlOperator(name, 1)


object SqlOperatorEq extends SqlInfixOperator("=")
object SqlOperatorLt extends SqlInfixOperator("<")
object SqlOperatorGt extends SqlInfixOperator(">")
object SqlOperatorLe extends SqlInfixOperator("<=")
object SqlOperatorGe extends SqlInfixOperator(">=")
object SqlOperatorNe extends SqlInfixOperator("<>")
object SqlOperatorNLt extends SqlInfixOperator("!<")
object SqlOperatorNGt extends SqlInfixOperator("!>")

object SqlOperatorAnd extends SqlInfixOperator("AND")
object SqlOperatorOr extends SqlInfixOperator("OR")
object SqlOperatorLike extends SqlInfixOperator("LIKE")
object SqlOperatorIn extends SqlInfixOperator("IN")
object SqlOperatorCat extends SqlInfixOperator("+")
object SqlOperatorPlus extends SqlInfixOperator("+")
object SqlOperatorMinus extends SqlInfixOperator("-")
object SqlOperatorMul extends SqlInfixOperator("*")
object SqlOperatorDiv extends SqlInfixOperator("/")
object SqlOperatorMod extends SqlInfixOperator("MOD")
object SqlOperatorBitNot extends SqlPrefixOperator("~")
object SqlOperatorBitAnd extends SqlInfixOperator("&")
object SqlOperatorBitOr extends SqlInfixOperator("|")
object SqlOperatorBitXor extends SqlInfixOperator("^")
object SqlOperatorAsg extends SqlInfixOperator("=")

object SqlOperatorNot extends SqlPrefixOperator("NOT")
object SqlOperatorIsNull extends SqlPostfixOperator("IS NULL")
object SqlOperatorIsNotNull extends SqlPostfixOperator("IS NOT NULL")
object SqlOperatorLength extends SqlPrefixOperator("LENGTH")

object SqlOperatorCount extends SqlPrefixOperator("COUNT")
object SqlOperatorSum extends SqlPrefixOperator("SUM")
object SqlOperatorAvg extends SqlPrefixOperator("AVG")
object SqlOperatorMin extends SqlPrefixOperator("MIN")
object SqlOperatorMax extends SqlPrefixOperator("MAX")
object SqlOperatorStdDev extends SqlPrefixOperator("StdDev")
object SqlOperatorStdDevP extends SqlPrefixOperator("StdDevP")
object SqlOperatorVar extends SqlPrefixOperator("Var")
object SqlOperatorVarP extends SqlPrefixOperator("VarP")


case class SqlTable(name: SqlTableName,
                    schema: Schema)


sealed abstract class CombineOp
case object CombineOpUnion extends CombineOp
case object CombineOpIntersect extends CombineOp
case object CombineOpExcept extends CombineOp


sealed abstract class SqlQuery {

  protected def writeAttributes(out: Writer, param: SqlWriteParameterization, attributes: Seq[SqlQuerySelectAttribute]) {
    if (attributes.isEmpty) {
      out.write("*")
    } else {
      writeJoined[SqlQuerySelectAttribute](out, attributes, ", ", {
        (out, attr) => attr.expr match {
          case SqlExprColumn(alias) if alias == attr.alias =>
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

  // TODO rename 'from' to 'tableRef' or similar
  protected def writeFrom(out: Writer, param: SqlWriteParameterization, from: Seq[SqlQuerySelectFrom]) {
    out.write("FROM ")
    writeJoined[SqlQuerySelectFrom](out, from, ", ", {
      (out, from) => from.query match {
        case q: SqlQueryTable => out.write(q.name)
        case q =>
          out.write("(")
          q.write(out, param)
          out.write(")")
      }
        writeAlias(out, from.alias)
    })
  }

  protected def writeWhere(out: Writer, param: SqlWriteParameterization, where: Seq[SqlExpr]) {
    out.write("WHERE ")
    writeJoined[SqlExpr](out, where, " AND ", {
      (out, expr) => expr.write(out, param)
    })
  }

  protected def writeGroupBy(out: Writer, param: SqlWriteParameterization, groupBys: Seq[SqlExpr]) {
    out.write("GROUP BY")
    writeJoined[SqlExpr](out, groupBys, ", ", {
      (out, groupBy) => groupBy.write(out, param)
    })
  }

  protected def writeHaving(out: Writer, param: SqlWriteParameterization, having: SqlExpr) {
    out.write("HAVING ")
    having.write(out, param)
  }

  protected def writeOrderBy(out: Writer, param: SqlWriteParameterization, orderBys: Seq[SqlQuerySelectOrderBy]) {
    out.write("ORDER BY ")
    writeJoined[SqlQuerySelectOrderBy](out, orderBys, ", ", {
      (out, orderBy) =>
        orderBy.expr.write(out, param)
        out.write(orderBy.order match {
          case Ascending => " ASC"
          case Descending => " DESC"
        })
    })
  }

  def write(out:Writer, param:SqlWriteParameterization) {
    this match {
      case SqlQueryTable(name) =>
        out.write("SELECT * FROM ")
        out.write(name)
      case s:SqlQuerySelect =>
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
      case s:SqlQueryCombine =>
        param.writeCombine(out, param, s)
      case SqlQueryEmpty =>
    }
  }
}

case class SqlQueryTable(name: SqlTableName) extends SqlQuery

case class SqlQuerySelect(options: Seq[String], // DISTINCT, ALL, etc.
                          attributes: Seq[SqlQuerySelectAttribute], // selected fields (expr + alias), empty seq for '*'
                          //                     isNullary: Boolean, // true if select represents nullary relation (?); in this case attributes should contain single dummy attribute (?)
                          from: Seq[SqlQuerySelectFrom], // FROM (
                          where: Seq[SqlExpr], // WHERE; Seq constructed from relational algebra
                          groupBy: Seq[SqlExpr], // GROUP BY
                          having: Option[SqlExpr], // HAVING
                          orderBy: Seq[SqlQuerySelectOrderBy], // ORDER BY
                          extra: Seq[String] // TOP n, etc.
                           ) extends SqlQuery

case class SqlQueryCombine(op: CombineOp,
                            left: SqlQuery,
                            right: SqlQuery) extends SqlQuery

case object SqlQueryEmpty extends SqlQuery // FIXME used when?

case class SqlQuerySelectAttribute(expr:SqlExpr, alias:Option[SqlColumnName])
case class SqlQuerySelectFrom(query:SqlQuery, alias:Option[SqlTableName])
case class SqlQuerySelectOrderBy(expr:SqlExpr, order:Order)

object SqlQuery {
  def defaultWriteCombine(out:Writer, param:SqlWriteParameterization, sqlCombine:SqlQueryCombine) {
    out.write('(')
    sqlCombine.left.write(out, param)
    out.write(") ")
    out.write(sqlCombine.op match {
      case CombineOpUnion => "UNION"
      case CombineOpIntersect => "INTERSECT"
      case CombineOpExcept => "EXCEPT"
    })
    out.write(" (")
    sqlCombine.right.write(out, param)
    out.write(")")
  }
}

// printing sql
trait SqlWriteParameterization {
  /**
   * write SqlQueryCombine to output sink
   *
   * @param out output sink
   * @param param this object for recursive calls
   * @param sqlCombine combining sql query
   */
  def writeCombine(out:Writer, param:SqlWriteParameterization, sqlCombine:SqlQueryCombine):Unit

  /**
   * write constant SQL expression (literal) to output sink
   *
   * @param out: output sink
   * @param value: constant value to write
   */
  def writeLiteral(out:Writer, value:SqlLiteral): Unit
}

