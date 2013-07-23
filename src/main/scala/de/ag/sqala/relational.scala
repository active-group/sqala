package de.ag.sqala

import de.ag.sqala.sql.Attribute

object relational {

  class Schema(val schema:Seq[(Attribute, Domain)]) {
    private lazy val environment = schema.toMap
    def dom(attribute:Attribute):Domain = environment(attribute)
    def degree = environment.size
    def difference(that:Schema):Schema = {
      val thatKeys = that.environment.keySet
      // keep order of attributes
      Schema(schema.filter(tuple => !thatKeys.contains(tuple._1)))
    }
    def isUnary = !schema.isEmpty && schema.tail.isEmpty
    def attributes:Seq[Attribute] = schema.map(_._1)
    def domains:Seq[Domain] = schema.map(_._2)
    // have own equals, etc. instead of case class to avoid construction of schemaMap
    override def equals(it:Any) = it match {
      case that:Schema => isComparable(that) && that.schema == this.schema
      case _ => false
    }
    override def hashCode() = schema.hashCode()
    def isComparable(that:Any) = that.isInstanceOf[Schema]
    def toEnvironment:Environment = new Environment(environment)
    override def toString = "Schema(%s)".format(
      schema.map{case (attr, domain) => "%s -> %s".format(attr, domain)}.mkString(", ")
    )
  }

  object Schema {
    val empty: Schema = Schema(Seq())

    // can have only one or the other apply, not both (same type after erasure);
    // settled for the one not requiring (...:*) for Seq arguments
    // def apply(schema:(Attribute, Domain)*) =
    //   new Schema(schema)
    def apply(schema:Seq[(Attribute, Domain)]) =
      new Schema(schema)
    def unapply(schema:Schema) = Some(schema.schema)
  }

  class Environment(val env: Map[Attribute, Domain]) {
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
      def subqueryDomain: (relational.Query) => Domain = {
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

  type QueryName = String

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

      def uiq(query: Query, query1: Query, query2: Query): relational.Schema = {
        val schema1 = rec(query1)
        domainCheck {fail =>
            if (!schema1.equals(rec(query2))) fail(schema1.toString, query)
        }
        schema1
      }

      def rec(q:Query): Schema = q match {
        case QueryEmpty => Schema.empty
        case b:QueryBase => b.base
        case QueryProject(subset, query) =>
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
        case QueryRestrict(expr, query) =>
          val schema = rec(query)
          domainCheck { fail =>
              val domain = toEnv(schema).expressionDomain(expr, domainCheck)
              if (!domain.isInstanceOf[Domain.Boolean.type]) fail("boolean", expr)
          }
          schema
        case QueryProduct(query1, query2) =>
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
        case QueryQuotient(query1, query2) =>
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
        case QueryUnion(q1, q2) => uiq(q, q1, q2)
        case QueryIntersection(q1, q2) => uiq(q, q1, q2)
        case QueryDifference(q1, q2) => uiq(q, q1, q2)
        case QueryGroupingProject(alist, query) =>
          val schema = rec(query)
          val environment = toEnv(schema)
          Schema(
            alist.map{case (attr, expr) =>
              val domain = environment.expressionDomain(expr, domainCheck)
              domainCheck { fail => if (domain.isInstanceOf[Domain.Product]) fail("non-product domain", domain) }
              (attr, domain)
            }
          )
        case QueryOrder(by, query) =>
          val schema = rec(query)
          val env = toEnv(schema)
          domainCheck { fail =>
              by.foreach { case (expr, order) =>
                val domain = env.expressionDomain(expr, domainCheck)
                if (!domain.isOrdered) fail("ordered domain", domain)}
          }
          schema
        case QueryTop(n, query) => rec(query)
      }
      rec(this)
    }
  }
  case object QueryEmpty extends Query
  case class QueryBase(name:QueryName,
                          base:Schema
                          /* TODO handle? */) extends Query
  case class QueryProject(subset:Seq[(Attribute, Expr)], query:Query) extends Query // map newly bound attributes to expressions
  case class QueryRestrict(expr:Expr /*returning boolean*/, query:Query) extends Query
  case class QueryProduct(query1:Query, query2:Query) extends Query
  case class QueryUnion(query1:Query, query2:Query) extends Query
  case class QueryIntersection(query1:Query, query2:Query) extends Query
  case class QueryQuotient(query1:Query, query2:Query) extends Query
  case class QueryDifference(query1:Query, query2:Query) extends Query
  /*
  ; the underlying query is grouped by the non-aggregate expressions in
  ; the grouping (hu? FIXME)
  */
  case class QueryGroupingProject(grouping:Seq[(Attribute, Expr)], query:Query) extends Query
  case class QueryOrder(by:Seq[(Expr, Order)], query:Query) extends Query
  case class QueryTop(n:Int, query:Query) extends Query // top n entries


  // TODO add make-extend

  //// Expressions

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
  case class ExprApplication(operator:Operator /*FIXME?*/, operands:Seq[Expr]) extends Expr
  case class ExprTuple(expressions:Seq[Expr]) extends Expr
  case class ExprAggregation(op:AggregationOperator, expr:Expr) extends Expr
  case class ExprCase(branches:Seq[CaseBranch], default:Option[Expr]) extends Expr
  case class ExprScalarSubQuery(query:Query /*FIXME*/) extends Expr
  case class ExprSetSubQuery(query:Query /*FIXME*/) extends Expr

  case class CaseBranch(condition:Expr, value:Expr)
}