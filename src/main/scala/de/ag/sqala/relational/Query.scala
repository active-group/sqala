package de.ag.sqala.relational

import de.ag.sqala._
import de.ag.sqala.relational.Schema.Attribute

/**
 *
 */
sealed abstract class Query {

  /**
   * Schema of query
   * @return Schema of query.  No checks are performed, but may throw NoSuchElementException
   *         if attribute reference refers to an unknown attribute.
   */
  def schema(): Schema = {
    schema(Environment.empty, IgnoringDomainChecker)
  }

  /**
   * Check domains of underlying schema. Throws DomainCheckException if there are domain
   * mismatches and NoSuchElementException if an attribute reference refers to an unknown attribute.
   */
  def checkDomains() {
    schema(Environment.empty, ExceptionThrowingDomainChecker)
  }

  /**
   * Schema of query, after checkDomains() is called.
   *
   * This is faster than calling checkDomains() and schema() in succession.
   *
   * @return Schema of query. See checkDomains() for exceptions that may be thrown.
   */
  def checkedSchema(): Schema = {
    schema(Environment.empty, ExceptionThrowingDomainChecker)
  }


  def schema(env:Environment, domainCheck:DomainChecker): Schema = {
    def toEnv(schema:Schema) =
      schema.toEnvironment.compose(env)

    def uiq(query: Query, query1: Query, query2: Query): Schema = {
      val schema1 = rec(query1)
      domainCheck {fail =>
        if (!schema1.equals(rec(query2))) fail(schema1.toString, query)
      }
      schema1
    }

    def rec(q:Query): Schema = q match {
      case Query.Empty => Schema.empty
      case b:Query.Base => b.base
      case Query.Project(subset, query) =>
        val baseSchema = rec(query)
        domainCheck { fail =>
          subset.foreach {
            case (attr, expr) => if (expr.isAggregate) fail("non-aggregate", expr)}
        }
        Schema(
          subset.map{case (attr, expr) =>
            val domain = toEnv(baseSchema).expressionDomain(expr, domainCheck)
            domainCheck { fail => if (domain.isInstanceOf[Domain.Product]) fail("non-product domain", domain) }
            (attr, domain)
          }.toSeq
        )
      case Query.Restrict(expr, query) =>
        val schema = rec(query)
        domainCheck { fail =>
          val domain = toEnv(schema).expressionDomain(expr, domainCheck)
          if (!domain.isInstanceOf[Domain.Boolean.type]) fail("boolean", expr)
        }
        schema
      case Query.Product(query1, query2) =>
        val schema1 = rec(query1)
        val schema2 = rec(query2)
        domainCheck { fail =>
          val env2 = schema2.toEnvironment
          // no two attributes with same name; avoid creating both environments
          schema1.schema.foreach {
            case (attr, domain) =>
              if (env2.contains(attr)) fail("non-duplicate " + attr, attr)
          }
        }
        Schema(schema1.schema ++ schema2.schema)
      case Query.Quotient(query1, query2) =>
        val schema1 = rec(query1)
        val schema2 = rec(query2)
        domainCheck { fail =>
          val env1 = schema1.toEnvironment
          schema2.schema.foreach{ case (attr2, domain2) =>
            env1.get(attr2) match {
              case None => fail("schema with attribute " + attr2, schema1)
              case Some(domain1) => if (!domain1.domainEquals(domain2))
                fail("environment where attribute %s is of domain %s".format(attr2, domain2),
                  "environment where attribute %s is of domain %s". format(attr2, domain1))
            }
          }
        }
        schema1.difference(schema2)
      case Query.Union(q1, q2) => uiq(q, q1, q2)
      case Query.Intersection(q1, q2) => uiq(q, q1, q2)
      case Query.Difference(q1, q2) => uiq(q, q1, q2)
      case Query.GroupingProject(alist, query) =>
        val schema = rec(query)
        val environment = toEnv(schema)
        Schema(
          alist.map{case (attr, expr) =>
            val domain = environment.expressionDomain(expr, domainCheck)
            domainCheck { fail => if (domain.isInstanceOf[Domain.Product]) fail("non-product domain", domain) }
            (attr, domain)
          }
        )
      case Query.Order(by, query) =>
        val schema = rec(query)
        val env = toEnv(schema)
        domainCheck { fail =>
          by.foreach { case (expr, order) =>
            val domain = env.expressionDomain(expr, domainCheck)
            if (!domain.isOrdered) fail("ordered domain", domain)}
        }
        schema
      case Query.Top(n, query) => rec(query)
    }
    rec(this)
  }
}

object Query {
  type Name = String
  case object Empty extends Query
  case class Base(name:Query.Name,
                       base:Schema
                       /* TODO handle? */) extends Query
  case class Project(subset:Seq[(Attribute, Expr)], query:Query) extends Query // map newly bound attributes to expressions
  case class Restrict(expr:Expr /*returning boolean*/, query:Query) extends Query
  case class Product(query1:Query, query2:Query) extends Query
  case class Union(query1:Query, query2:Query) extends Query
  case class Intersection(query1:Query, query2:Query) extends Query
  case class Quotient(query1:Query, query2:Query) extends Query
  case class Difference(query1:Query, query2:Query) extends Query
  /*
  ; the underlying query is grouped by the non-aggregate expressions in
  ; the grouping (hu? FIXME)
  */
  case class GroupingProject(grouping:Seq[(Attribute, Expr)], query:Query) extends Query
  case class Order(by:Seq[(Expr, OrderDirection)], query:Query) extends Query
  case class Top(n:Int, query:Query) extends Query // top n entries

  // TODO add make-extend
}