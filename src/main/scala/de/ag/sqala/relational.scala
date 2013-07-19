package de.ag.sqala

import de.ag.sqala.sql.{Attribute, Table}

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

  sealed abstract class FailedArg // FIXME maybe just use Any
  case class FailedSchema(schema:Schema) extends FailedArg
  case class FailedDomain(domain:Domain) extends FailedArg
  case class FailedExpr(expr:Expr) extends FailedArg
  case class FailedAttribute(attr:Attribute) extends FailedArg
  case class FailedQuery(query:Query) extends FailedArg
  type FailProc = Option[(String, FailedArg)=>Unit]

  class Environment(val env: Map[Attribute, Domain]) {
    def lookup(key:Attribute):Domain = env(key) // throws NoSuchElementException if no such key

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

    def expressionDomain(expr:Expr, failProc:FailProc): Domain = {
      def subqueryDomain: (relational.Query) => Domain = {
        subquery =>
          val schema = subquery.schema(this, failProc)
          failProc match {
            case None =>
            case Some(fail) => if (!schema.isUnary) fail("unary-relation", FailedSchema(schema))
          }
          schema.schema.head._2

      }
      expr.fold(
        onAttributeRef= {name => lookup(name)},
        onConst= {(domain, value) => domain},
        onNull= {domain => domain},
        onApplication= {(rator:Operator, rands:Seq[Domain]) => rator.rangeType(failProc, rands)},
        onTuple= {domains:Seq[Domain] => DBProduct(domains)},
        onAggregation= {
          (aggOpOrString:Either[AggregationOp, String], domain:Domain) =>
            aggOpOrString match {
              case Left(aggOp) => aggOp match {
                case AggregationOpCount => DBInteger
                case _ =>
                  failProc match { // FIXME fail check stuff should go to op, shouldn't it?
                    case None =>
                    case Some(fail) => aggOp match {
                      case AggregationOpSum
                           | AggregationOpAvg
                           | AggregationOpStdDev
                           | AggregationOpStdDevP
                           | AggregationOpVar
                           | AggregationOpVarP =>
                        if (!domain.isNumeric) fail("non-numeric", FailedDomain(domain))
                      case AggregationOpMin
                           | AggregationOpMax =>
                        if (!domain.isOrdered) fail("non-ordered", FailedDomain(domain))
                      case AggregationOpCount =>
                    }
                  }
                  domain
              }
              case Right(otherOp) => domain /* FIXME that's certainly wrong, need type of op named via string! */
            }},
        onCase= {
          (branches:Seq[(Domain, Domain)], default:Option[Domain]) =>
            val domain = default match {
              case None => branches.head._1
              case Some(dom) => dom
            }
            failProc match {
              case None =>
              case Some(fail) =>
                branches.foreach {
                  case (condition, value) =>
                    if (!condition.isInstanceOf[DBBoolean.type]) fail("non-boolean", FailedDomain(condition))
                    if (!value.equals(domain)) fail(value.name, FailedDomain(domain)) // FIXME first arg of fail seems 'inverted', should be "non-" + value.name (?)
                }
            }
            domain},
        onScalarSubQuery= subqueryDomain,
        onSetSubQuery= subqueryDomain)
    }
  }

  object Environment {
    val empty = new Environment(Map())
  }

  type QueryName = String

  sealed abstract class Query
  case object QueryEmpty extends Query
  case class QueryBase(name:QueryName,
                          schema:Schema,
                          table:Option[Table]) extends Query
  case class QueryProject(subset:Set[(Attribute, Expr)], query:Query) extends Query // map newly bound attributes to expressions
  case class QueryRestrict(expr:Expr /*returning boolean*/, query:Query) extends Query
  case class QueryProduct(query1:Query, query2:Query) extends Query
  case class QueryUnion(query1:Query, query2:Query) extends Query
  case class QueryIntersection(query1:Query, query2:Query) extends Query
  case class QueryQuotient(query1:Query, query2:Query) extends Query
  case class QueryDifference(query1:Query, query2:Query) extends Query
  /*
  ; the underlying query is grouped by the non-aggregate expressions in
  ; the alist (hu? FIXME)
  */
  case class QueryGroupingProject(alist:Any /*FIXME*/, query:Query) extends Query
  case class QueryOrder(by:Seq[(Attribute, Order)], query:Query) extends Query
  case class QueryTop(n:Int) extends Query // top n entries

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
          default.map(_.isAggregate).getOrElse(true)
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
                onAggregation: (Either[AggregationOp, String], T) => T,
                onCase: (Seq[(T, T)], Option[T]) => T,
                onScalarSubQuery: (Query) => T,
                onSetSubQuery: (Query) => T):T = {
      def rec(expr:Expr):T = this match {
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
  case class ExprAggregation(op:Either[AggregationOp, String], expr:Expr) extends Expr
  case class ExprCase(branches:Seq[CaseBranch], default:Option[Expr]) extends Expr
  case class ExprScalarSubQuery(query:Query /*FIXME*/) extends Expr
  case class ExprSetSubQuery(query:Query /*FIXME*/) extends Expr


  sealed abstract class AggregationOp
  case object AggregationOpCount extends AggregationOp
  case object AggregationOpSum extends AggregationOp
  case object AggregationOpAvg extends AggregationOp
  case object AggregationOpMin extends AggregationOp
  case object AggregationOpMax extends AggregationOp
  case object AggregationOpStdDev extends AggregationOp
  case object AggregationOpStdDevP extends AggregationOp
  case object AggregationOpVar extends AggregationOp
  case object AggregationOpVarP extends AggregationOp

  case class Operator(name: String,
                         rangeType: (FailProc, Seq[Domain])=> Domain,
                         proc: Any, /* FIXME Scala implementation of operator (?) */
                         data:Any /* FIXME? domain-specific data for outside use (?)*/ )

  case class CaseBranch(condition:Expr, value:Expr)
}