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
  def writeLiteral(out:Writer, literal:Literal): Unit
}

object defaultSqlWriteParameterization extends WriteParameterization {
  /**
   * write QueryCombine to output sink
   *
   * @param out output sink
   * @param param this object for recursive calls
   * @param combine combining  query
   */
  def writeCombine(out: Writer, param: WriteParameterization, combine: Query.Combine) {
    Query.defaultWriteCombine(out, param, combine)
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