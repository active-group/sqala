package de.ag.sqala.relational

import de.ag.sqala.{AggregationOperator, Operator, Domain}

/**
 *
 */
sealed abstract class Expr {
  def isAggregate:Boolean = this match {
    case _:ExprAttributeRef
         | _:ExprConst
         | _:ExprNull
         | _:ExprScalarSubQuery
         | _:ExprSetSubQuery => false
    case _:ExprAggregation => true
    case ExprApplication(rator, rands) =>
      rands.exists(_.isAggregate)
    case ExprTuple(exprs) => exprs.exists(_.isAggregate)
    case ExprCase(branches, default) =>
      branches.exists{b => b.condition.isAggregate || b.value.isAggregate} ||
        default.exists(_.isAggregate)
  }

  /*
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
      case ExprAttributeRef(name) => onAttributeRef(name)
      case ExprConst(domain, value) => onConst(domain, value)
      case ExprNull(typ) => onNull(typ)
      case ExprAggregation(op, exp) => onAggregation(op, rec(exp))
      case ExprApplication(rator, rands) => onApplication(rator, rands.map(rec))
      case ExprTuple(exprs) => onTuple(exprs.map(rec))
      case ExprCase(branches, default) => onCase(branches.map{case CaseBranch(condition, value) => (rec(condition), rec(value))}, default.map(rec))
      case ExprScalarSubQuery(query) => onScalarSubQuery(query)
      case ExprSetSubQuery(query) => onSetSubQuery(query)
    }
    rec(this)
  }
}

case class ExprAttributeRef(name:String) extends Expr
case class ExprConst(domain:Domain, value:String /*FIXME?*/) extends Expr
case class ExprNull(typ:Domain) extends Expr
case class ExprApplication(operator:Operator, operands:Seq[Expr]) extends Expr
case class ExprTuple(expressions:Seq[Expr]) extends Expr
case class ExprAggregation(op:AggregationOperator, expr:Expr) extends Expr
case class ExprCase(branches:Seq[CaseBranch], default:Option[Expr]) extends Expr
case class ExprScalarSubQuery(query:Query) extends Expr
case class ExprSetSubQuery(query:Query) extends Expr

case class CaseBranch(condition:Expr, value:Expr)
