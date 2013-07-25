package de.ag.sqala.sql

import java.io.{StringWriter, Writer}
import de.ag.sqala.StringUtils._
import scala.Some
import de.ag.sqala.{Operator, InfixOperator, PrefixOperator, PostfixOperator}

/**
 *
 */

case class ExprCaseBranch(condition: Expr, expr: Expr)

sealed abstract class Expr {
  def write(out:Writer, param:WriteParameterization) {
    this match {
      case ExprConst(value) =>
        param.writeLiteral(out, value)
      case ExprTuple(exprs) =>
        out.write("(")
        writeJoined[Expr](out, exprs, ", ", {
          (out, expr) => expr.write(out, param)})
      case ExprColumn(columnName) =>
        out.write(columnName)
      case ExprApp(operator, operands) =>
        operator match {
          case PostfixOperator(opName) =>
            out.write("(")
            operands.head.write(out, param)
            writeSpace(out)
            out.write(opName)
            out.write(")")
          case PrefixOperator(opName) =>
            out.write(opName)
            out.write("(")
            operands.head.write(out, param)
            out.write(")")
          case InfixOperator(opName) =>
            out.write("(")
            operands.head.write(out, param)
            writeSpace(out)
            out.write(opName)
            writeSpace(out)
            operands.tail.head.write(out, param)
            out.write(")")
        }
      case ExprCase(branches, default) =>
        out.write("(CASE ")
        branches.foreach(writeBranch(out, param, _))
        default match {
          case None =>
          case Some(expr) =>
            out.write(" ELSE ")
            expr.write(out, param)
        }
        out.write(")")
      case ExprExists(query) =>
        out.write("EXISTS (")
        query.write(out, param)
        out.write(")")
      case ExprSubQuery(query) =>
        out.write("(")
        query.write(out, param)
        out.write(")")
    }
  }

  protected def writeBranch(out:Writer, param:WriteParameterization, branch:ExprCaseBranch) {
    out.write("WHEN ")
    branch.condition.write(out, param)
    out.write(" THEN ")
    branch.expr.write(out, param)
  }

  def toString(param:WriteParameterization) = {
    val result = new StringWriter()
    write(result, param)
    result.toString
  }

  override def toString = toString(defaultSqlWriteParameterization)
}

case class ExprConst(value: Literal) extends Expr
case class ExprTuple(exprs: Seq[Expr]) extends Expr
case class ExprColumn(name: Query.ColumnName) extends Expr
case class ExprApp(operator: Operator, operands: Seq[Expr]) extends Expr
case class ExprCase(branches: Seq[ExprCaseBranch], default: Option[Expr]) extends Expr
case class ExprExists(query: Query) extends Expr
case class ExprSubQuery(query: Query) extends Expr

sealed abstract class Literal
case class LiteralInteger(i:Int) extends Literal
case class LiteralDouble(d:Double) extends Literal
case class LiteralDecimal(d:BigDecimal) extends Literal
case class LiteralString(s:String) extends Literal
case object LiteralNull extends Literal
case class LiteralBoolean(b:Boolean) extends Literal

sealed abstract class CombineOp
case object CombineOpUnion extends CombineOp
case object CombineOpIntersect extends CombineOp
case object CombineOpExcept extends CombineOp


