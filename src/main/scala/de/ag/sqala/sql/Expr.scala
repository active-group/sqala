package de.ag.sqala.sql

import java.io.{StringWriter, Writer}
import de.ag.sqala.StringUtils._
import scala.Some
import de.ag.sqala.{Operator, InfixOperator, PrefixOperator, PostfixOperator}

/**
 * Expressions in SQL
 */
sealed abstract class Expr {
  /**
   * Serialize string to output sink, using write parameterization for some clauses.
   *
   * @param out    output sink
   * @param param  (database specific) write parameterization
   */
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
      case Expr.Exists(table) =>
        out.write("EXISTS (")
        table.write(out, param)
        out.write(")")
      case Expr.SubTable(table) =>
        out.write("(")
        table.write(out, param)
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
  /** constant value */
  case class Const(value: Literal) extends Expr
  /** tuple */
  case class Tuple(exprs: Seq[Expr]) extends Expr
  /** column reference */
  case class Column(name: Table.ColumnName) extends Expr
  /** function application */
  case class App(operator: Operator, operands: Seq[Expr]) extends Expr
  /** case (aka 'switch') */
  case class Case(branches: Seq[CaseBranch], default: Option[Expr]) extends Expr
  /** 'EXISTS' clause */
  case class Exists(table: Table) extends Expr
  /** sub-select */
  case class SubTable(table: Table) extends Expr

  /** Literal in SQL expression */
  sealed abstract class Literal
  object Literal {
    /** An integer */
    case class Integer(i:scala.Int) extends Literal
    /** A double */
    case class Double(d:scala.Double) extends Literal
    /** A arbitrary precision decimal */
    case class Decimal(d:scala.BigDecimal) extends Literal
    /** A String */
    case class String(s:scala.Predef.String) extends Literal
    /** The database 'NULL' */
    case object Null extends Literal
    /** A boolean */
    case class Boolean(b:scala.Boolean) extends Literal
  }

  /** Combine operator in SQL expression.
    * Could have been enumeration. */
  sealed abstract class CombineOp {
    def toSpacedString:String = " " + this.toString + " "

    override def toString: String = this match {
      case CombineOp.Union => "UNION"
      case CombineOp.Intersect => "INTERSECT"
      case CombineOp.Except => "EXCEPT"
      case CombineOp.UnionAll => "UNION ALL"
    }
  }

  object CombineOp {
    /** Union */
    case object Union extends CombineOp
    /** Union all */
    case object UnionAll extends CombineOp
    /** Intersection */
    case object Intersect extends CombineOp
    /** Except */
    case object Except extends CombineOp
  }

  /** branch in SQL case expression */
  case class CaseBranch(condition: Expr, expr: Expr)
}