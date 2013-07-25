package de.ag.sqala.sql

import java.io.{StringWriter, Writer}
import de.ag.sqala.StringUtils._
import scala.Some
import de.ag.sqala.{Operator, InfixOperator, PrefixOperator, PostfixOperator}

/**
 *
 */
sealed abstract class Expr {
  def write(out:Writer, param:WriteParameterization) {
    this match {
      case Expr.Const(value) =>
        param.writeLiteral(out, value)
      case Expr.Tuple(exprs) =>
        out.write("(")
        writeJoined[Expr](out, exprs, ", ", {
          (out, expr) => expr.write(out, param)})
      case Expr.Column(columnName) =>
        out.write(columnName)
      case Expr.App(operator, operands) =>
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
      case Expr.Case(branches, default) =>
        out.write("(CASE ")
        branches.foreach(writeBranch(out, param, _))
        default match {
          case None =>
          case Some(expr) =>
            out.write(" ELSE ")
            expr.write(out, param)
        }
        out.write(")")
      case Expr.Exists(query) =>
        out.write("EXISTS (")
        query.write(out, param)
        out.write(")")
      case Expr.SubQuery(query) =>
        out.write("(")
        query.write(out, param)
        out.write(")")
    }
  }

  protected def writeBranch(out:Writer, param:WriteParameterization, branch:Expr.CaseBranch) {
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

object Expr {
  case class Const(value: Literal) extends Expr
  case class Tuple(exprs: Seq[Expr]) extends Expr
  case class Column(name: Query.ColumnName) extends Expr
  case class App(operator: Operator, operands: Seq[Expr]) extends Expr
  case class Case(branches: Seq[CaseBranch], default: Option[Expr]) extends Expr
  case class Exists(query: Query) extends Expr
  case class SubQuery(query: Query) extends Expr

  sealed abstract class Literal
  object Literal {
    case class Integer(i:scala.Int) extends Literal
    case class Double(d:scala.Double) extends Literal
    case class Decimal(d:scala.BigDecimal) extends Literal
    case class String(s:scala.Predef.String) extends Literal
    case object Null extends Literal
    case class Boolean(b:scala.Boolean) extends Literal
  }

  sealed abstract class CombineOp
  object CombineOp {
    case object Union extends CombineOp
    case object Intersect extends CombineOp
    case object Except extends CombineOp
  }

  case class CaseBranch(condition: Expr, expr: Expr)
}