package de.ag.sqala.relational

import de.ag.sqala.{AggregationOperator, Operator, DomainChecker, Domain}
import de.ag.sqala.relational.Schema.Attribute
/**
 *
 */
class Environment(val env: Map[Schema.Attribute, Domain]) {
  /**
   * Lookup attribute's domain
   * @param key  attribute to lookup
   * @return     domain of attribute; throws NoSuchElementException if there is no such attribute
   */
  def lookup(key:Attribute):Domain = env(key)

  /**
   * Lookup attribute's domain, if any
   *
   * @param attribute attribute to lookup
   * @return          domain of attribute, if any
   */
  def get(attribute:Attribute):Option[Domain] = env.get(attribute)

  override def toString = env.toString()

  /**
   * Check if there is a certain attribute
   * @param attribute attribute to lookup
   * @return          true iff there is such an attribute, false otherwise
   */
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

  /**
   * Domain of an expression under this environment
   * @param expr         expression
   * @param domainCheck  DomainChecker to apply; use DomainChecker.IgnoringDomainChecker to not check at all
   * @return             Domain of expression; may throw DomainCheckException if DomainChecker is used,
   *                     NoSuchElementException if an non-existing attribute is referred,
   *                     or other exceptions if no domain check is performed, but the expression is not sound.
   */
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
  /** The empty environment */
  val empty = new Environment(Map())
}
