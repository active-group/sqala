package de.ag.sqala

import java.io.Writer

class Label(val label:String) // FIXME type alias?

class Domain(val typ: String)  // FIXME structured?

case class SqlExprCaseBranch(condition: SqlExpr, value: SqlExpr)


sealed abstract class SqlExpr

case class SqlExprConst(value: SqlLiteral) extends SqlExpr

case class SqlExprTuple(values: Seq[String]) extends SqlExpr

case class SqlExprColumn(name: SqlColumnName) extends SqlExpr

case class SqlExprApp(operator: SqlOperator, operands: Seq[SqlExpr]) extends SqlExpr

case class SqlExprCase(branches: Seq[SqlExprCaseBranch], default: Option[SqlExpr]) extends SqlExpr

case class SqlExprExists(select: SqlQuery) extends SqlExpr

case class SqlExprSubQuery(select: SqlQuery) extends SqlExpr

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


class CombineOp  // TODO


sealed abstract class SqlQuery

case class SqlQueryTable(name: SqlTableName) extends SqlQuery

case class SqlQuerySelect(options: Seq[String], // DISTINCT, ALL, etc.
                          attributes: Seq[(SqlExpr, SqlColumnName)], // selected fields (expr + alias), empty seq for '*'
                          //                     isNullary: Boolean, // true if select represents nullary relation (?); in this case attributes should contain single dummy attribute (?)
                          from: Seq[(SqlQuery, Option[SqlTableName])], // FROM (
                          where: Seq[SqlExpr], // WHERE
                          groupBy: Seq[SqlExpr], // GROUP BY
                          having: Seq[SqlExpr], // HAVING
                          orderBy: Seq[(SqlExpr, Order)], // ORDER BY
                          extra: Seq[String] // TOP n, etc.
                           ) extends SqlQuery

case class SqlQueryCombine(op: CombineOp,
                            left: SqlQuery,
                            right: SqlQuery) extends SqlQuery

case object SqlQueryEmpty extends SqlQuery // FIXME used when?


// printing sql
trait SqlWriteParameterization {
  /**
   * write SqlQueryCombine to output sink
   *
   * @param out output sink
   * @param param this object for recursive calls
   * @param sqlCombine left sql query, combine operator, right sql query
   */
  def writeCombine(out:Writer, param:SqlWriteParameterization, sqlCombine:(SqlQuery, CombineOp, SqlQuery)):Unit

  /**
   * write constant SQL expression (literal) to output sink
   *
   * @param out: output sink
   * @param value: constant value to write
   */
  def writeLiteral(out:Writer, value:SqlLiteral): Unit
}

