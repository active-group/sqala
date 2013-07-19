package de.ag.sqala

import java.io.{StringWriter, Writer}
import de.ag.sqala.StringUtils._
import de.ag.sqala.relational.Schema

object sql {
  type TableName = String

  type ColumnName = String

  type Label = String
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
  case class ExprColumn(name: ColumnName) extends Expr
  case class ExprApp(operator: Operator, operands: Seq[Expr]) extends Expr
  case class ExprCase(branches: Seq[ExprCaseBranch], default: Option[Expr]) extends Expr
  case class ExprExists(query: Query) extends Expr
  case class ExprSubQuery(query: Query) extends Expr

  sealed abstract class Literal
  case class LiteralNumber(n:BigDecimal) extends Literal // FIXME other type than BigDecimal?
  case class LiteralString(s:String) extends Literal
  case object LiteralNull extends Literal
  case class LiteralBoolean(b:Boolean) extends Literal

  sealed abstract class Operator(val name: String, val arity: Int)
  case class PrefixOperator(override val name: String) extends Operator(name, 1)
  case class InfixOperator(override val name: String) extends Operator(name, 2)
  case class PostfixOperator(override val name: String) extends Operator(name, 1)

  object OperatorEq extends InfixOperator("=")
  object OperatorLt extends InfixOperator("<")
  object OperatorGt extends InfixOperator(">")
  object OperatorLe extends InfixOperator("<=")
  object OperatorGe extends InfixOperator(">=")
  object OperatorNe extends InfixOperator("<>")
  object OperatorNLt extends InfixOperator("!<")
  object OperatorNGt extends InfixOperator("!>")

  object OperatorAnd extends InfixOperator("AND")
  object OperatorOr extends InfixOperator("OR")
  object OperatorLike extends InfixOperator("LIKE")
  object OperatorIn extends InfixOperator("IN")
  object OperatorCat extends InfixOperator("+")
  object OperatorPlus extends InfixOperator("+")
  object OperatorMinus extends InfixOperator("-")
  object OperatorMul extends InfixOperator("*")
  object OperatorDiv extends InfixOperator("/")
  object OperatorMod extends InfixOperator("MOD")
  object OperatorBitNot extends PrefixOperator("~")
  object OperatorBitAnd extends InfixOperator("&")
  object OperatorBitOr extends InfixOperator("|")
  object OperatorBitXor extends InfixOperator("^")
  object OperatorAsg extends InfixOperator("=")

  object OperatorNot extends PrefixOperator("NOT")
  object OperatorIsNull extends PostfixOperator("IS NULL")
  object OperatorIsNotNull extends PostfixOperator("IS NOT NULL")
  object OperatorLength extends PrefixOperator("LENGTH")

  object OperatorCount extends PrefixOperator("COUNT")
  object OperatorSum extends PrefixOperator("SUM")
  object OperatorAvg extends PrefixOperator("AVG")
  object OperatorMin extends PrefixOperator("MIN")
  object OperatorMax extends PrefixOperator("MAX")
  object OperatorStdDev extends PrefixOperator("StdDev")
  object OperatorStdDevP extends PrefixOperator("StdDevP")
  object OperatorVar extends PrefixOperator("Var")
  object OperatorVarP extends PrefixOperator("VarP")


  case class Table(name: TableName,
                      schema: Schema)


  sealed abstract class CombineOp
  case object CombineOpUnion extends CombineOp
  case object CombineOpIntersect extends CombineOp
  case object CombineOpExcept extends CombineOp


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

    // TODO rename 'from' to 'tableRef' or similar
    protected def writeFrom(out: Writer, param: WriteParameterization, from: Seq[QuerySelectFrom]) {
      out.write("FROM ")
      writeJoined[QuerySelectFrom](out, from, ", ", {
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

  case class QueryTable(name: TableName) extends Query

  case class QuerySelect(options: Seq[String], // DISTINCT, ALL, etc.
                            attributes: Seq[QuerySelectAttribute], // selected fields (expr + alias), empty seq for '*'
                            //                     isNullary: Boolean, // true if select represents nullary relation (?); in this case attributes should contain single dummy attribute (?)
                            from: Seq[QuerySelectFrom], // FROM (
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

  case class QuerySelectAttribute(expr:Expr, alias:Option[ColumnName])
  case class QuerySelectFrom(query:Query, alias:Option[TableName])
  case class QuerySelectOrderBy(expr:Expr, order:Order)

  object Query {
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
        case LiteralNumber(n) => out.write(n.toString())
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
                            from:Seq[QuerySelectFrom],
                            where: Seq[Expr]=Seq(),
                            groupBy: Seq[Expr]=Seq(),
                            having: Option[Expr]=None,
                            orderBy: Seq[QuerySelectOrderBy]=Seq(),
                            extra:Seq[String]=Seq()) = {
      QuerySelect(options, attributes, from, where, groupBy, having, orderBy, extra)
    }
  }

  // printing
  trait WriteParameterization {
    /**
     * write QueryCombine to output sink
     *
     * @param out output sink
     * @param param this object for recursive calls
     * @param Combine combining  query
     */
    def writeCombine(out:Writer, param:WriteParameterization, Combine:QueryCombine):Unit

    /**
     * write constant  expression (literal) to output sink
     *
     * @param out: output sink
     * @param literal: constant literal to write
     */
    def writeLiteral(out:Writer, literal:Literal): Unit
  }

  object defaultSqlWriteParameterization extends WriteParameterization {
    /**
     * write QueryCombine to output sink
     *
     * @param out output sink
     * @param param this object for recursive calls
     * @param Combine combining  query
     */
    def writeCombine(out: Writer, param: WriteParameterization, Combine: QueryCombine) {
      Query.defaultWriteCombine(out, param, Combine)
    }

    /**
     * write constant  expression (literal) to output sink
     *
     * @param out: output sink
     * @param literal: constant literal to write
     */
    def writeLiteral(out: Writer, literal: Literal) {
      Query.defaultWriteLiteral(out, literal)
    }
  }
}