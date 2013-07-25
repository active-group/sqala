package de.ag.sqala.relational

import de.ag.sqala.{AggregationOperator, Operator, Domain}

/**
 * Expression in a relational algebra
 */
sealed abstract class Expr {
  /**
   * Check if expression contains Expr.Aggregation
   * @return
   */
  def isAggregate:Boolean = this match {
    case _:Expr.AttributeRef
         | _:Expr.Const
         | _:Expr.Null
         | _:Expr.ScalarSubQuery
         | _:Expr.SetSubQuery => false
    case _:Expr.Aggregation => true
    case Expr.Application(rator, rands) =>
      rands.exists(_.isAggregate)
    case Expr.Tuple(exprs) => exprs.exists(_.isAggregate)
    case Expr.Case(branches, default) =>
      branches.exists{b => b.condition.isAggregate || b.value.isAggregate} ||
        default.exists(_.isAggregate)
  }

  /**
   * (Right-)Fold over an expression (catamorphism).
   *
   * This implementation currently uses recursion.
   *
   * @param onAttributeRef   fold Expr.AttributeRef
   * @param onConst          fold Expr.Const
   * @param onNull           fold Expr.Null
   * @param onApplication    fold Expr.Application
   * @param onTuple          fold Expr.Tuple
   * @param onAggregation    fold Expr.Aggregation
   * @param onCase           fold Expr.Case
   * @param onScalarSubQuery fold Expr.ScalarSubQuery
   * @param onSetSubQuery    fold Expr.SetSubQuery
   * @tparam T               type of result
   * @return                 right-fold of expression
   */
  /* Copy this to help calling
      onAttributeRef= {name => ???},
      onConst= {value => ???},
      onNull= {domain => ???},
      onApplication= {(rator, rands) => ???},
      onTuple= {values => ???},
      onAggregation= {(aggOpOrString, expr) => ???},
      onCase= {(branches, default) => ???},
      onScalarSubQuery= {query => ???},
      onSetSubQuery= {query => ???})
  */
  def fold[T](onAttributeRef: (String) => T,
              onConst: (Domain, String) => T,
              onNull: (Domain) => T,
              onApplication: (Operator, Seq[T]) => T,
              onTuple: (Seq[T]) => T,
              onAggregation: (AggregationOperator, T) => T,
              onCase: (Seq[(T, T)], Option[T]) => T,
              onScalarSubQuery: (Query) => T,
              onSetSubQuery: (Query) => T):T = {
    def rec(expr:Expr):T = expr match {
      case Expr.AttributeRef(name) => onAttributeRef(name)
      case Expr.Const(domain, value) => onConst(domain, value)
      case Expr.Null(typ) => onNull(typ)
      case Expr.Aggregation(op, exp) => onAggregation(op, rec(exp))
      case Expr.Application(rator, rands) => onApplication(rator, rands.map(rec))
      case Expr.Tuple(exprs) => onTuple(exprs.map(rec))
      case Expr.Case(branches, default) => onCase(branches.map{case Expr.CaseBranch(condition, value) => (rec(condition), rec(value))}, default.map(rec))
      case Expr.ScalarSubQuery(query) => onScalarSubQuery(query)
      case Expr.SetSubQuery(query) => onSetSubQuery(query)
    }
    rec(this)
  }
}

object Expr {
  /** attribute reference */
  case class AttributeRef(name:String) extends Expr
  /** constant value */
  case class Const(domain:Domain, value:String /*FIXME?*/) extends Expr
  /** a nullable domain */
  case class Null(typ:Domain) extends Expr
  /** application (but not with aggregate op FIXME) */
  case class Application(operator:Operator, operands:Seq[Expr]) extends Expr
  /** tuples */
  case class Tuple(expressions:Seq[Expr]) extends Expr
  /** application with aggregation operator (count, sum, etc.) */
  case class Aggregation(op:AggregationOperator, expr:Expr) extends Expr
  /** case ('switch') */
  case class Case(branches:Seq[CaseBranch], default:Option[Expr]) extends Expr
  /** sub-query returning a scalar */
  case class ScalarSubQuery(query:Query) extends Expr
  /** sub-query returning a set */
  case class SetSubQuery(query:Query) extends Expr

  /** branch of a case */
  case class CaseBranch(condition:Expr, value:Expr)
}