package de.ag.sqala.sql

import java.io.Writer

/**
 *
 */
trait WriteParameterization {
  /**
   * write QueryCombine to output sink
   *
   * @param out output sink
   * @param param this object for recursive calls
   * @param Combine combining  query
   */
  def writeCombine(out:Writer, param:WriteParameterization, Combine:Query.Combine):Unit

  /**
   * write constant  expression (literal) to output sink
   *
   * @param out: output sink
   * @param literal: constant literal to write
   */
  def writeLiteral(out:Writer, literal:Expr.Literal): Unit
}

object defaultSqlWriteParameterization extends WriteParameterization {
  /**
   * write QueryCombine to output sink
   *
   * @param out     output sink
   * @param param   this object for recursive calls
   * @param combine Query.combine to write
   */
  def writeCombine(out:Writer, param:WriteParameterization, combine:Query.Combine) {
    out.write('(')
    combine.left.write(out, param)
    out.write(") ")
    out.write(combine.op match {
      case Expr.CombineOp.Union => "UNION"
      case Expr.CombineOp.Intersect => "INTERSECT"
      case Expr.CombineOp.Except => "EXCEPT"
    })
    out.write(" (")
    combine.right.write(out, param)
    out.write(")")
  }
  /**
   * write constant  expression (literal) to output sink
   *
   * @param out     output sink
   * @param literal Expr.Literal  to write
   */
  def writeLiteral(out: Writer, literal: Expr.Literal) {
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
}