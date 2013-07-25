package de.ag.sqala.relational

import de.ag.sqala.{AggregationOperator, Operator, DomainChecker, Domain}
import de.ag.sqala.relational.Schema.Attribute
/**
 *
 */
class Environment(val env: Map[Schema.Attribute, Domain]) {
  def lookup(key:Attribute):Domain = env(key) // throws NoSuchElementException if no such key
  def get(attribute:Attribute):Option[Domain] = env.get(attribute)
  override def toString = env.toString()

  def contains(attribute:Attribute) = env.contains(attribute)
  /**
   * compose two environments to one; environments must not use same attributes
   * @param that  environment to add
   * @return      environment that contains attributes and domains of both environments
   */
  def compose(that:Environment): Environment = {
    // micro-optimize common cases
    if (this.env.size == 0) that
    else if (that.env.size == 0) this
    else new Environment(this.env ++ that.env)
  }

  def expressionDomain(expr:Expr, domainCheck:DomainChecker): Domain = {
    def subqueryDomain: (Query) => Domain = {
      subquery =>
        val schema = subquery.schema(this, domainCheck)
        domainCheck { fail => if (!schema.isUnary) fail("unary-relation", schema) }
        schema.schema.head._2
    }
    expr.fold(
      onAttributeRef= {name => lookup(name)},
      onConst= {(domain, value) => domain},
      onNull= {domain => domain},
      onApplication= {(rator:Operator, rands:Seq[Domain]) => rator.rangeDomain(domainCheck, rands)},
      onTuple= {domains:Seq[Domain] => Domain.Product(domains)},
      onAggregation= {(aggOp: AggregationOperator, operand: Domain) => aggOp.rangeDomain(domainCheck, operand)},
      onCase= {
        (branches:Seq[(Domain, Domain)], default:Option[Domain]) =>
          val domain = default match {
            case None => branches.head._2
            case Some(dom) => dom
          }
          domainCheck {fail =>
            branches.foreach {
              case (condition, value) =>
                if (!condition.isInstanceOf[Domain.Boolean.type]) fail(Domain.Boolean, condition)
                if (!value.equals(domain)) fail(domain, value)
            }
          }
          domain},
      onScalarSubQuery= subqueryDomain,
      onSetSubQuery= { query => Domain.Set(subqueryDomain(query)) })
  }
}

object Environment {
  val empty = new Environment(Map())
}
