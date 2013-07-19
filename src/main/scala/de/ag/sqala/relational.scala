package de.ag.sqala

import de.ag.sqala.sql.{Label, Table}

object relational {

  class Schema(val schema:Seq[(Label, Domain)]) {
    private lazy val schemaMap = schema.toMap
    def dom(label:Label):Domain = schemaMap(label)
    def degree = schemaMap.size
    def difference(that:Schema):Schema = {
      val thatKeys = that.schemaMap.keySet
      // keep order of labels
      new Schema(schema.filter(ld => !thatKeys.contains(ld._1)))
    }
    def isUnary = degree == 1
    def labels:Seq[Label] = schema.map(_._1)
    def domains:Seq[Domain] = schema.map(_._2)
  }

  class QueryName(name:String) // FIXME type alias?

  sealed abstract class Query
  case object QueryEmpty extends Query
  case class QueryBase(name:QueryName,
                          schema:Schema,
                          table:Option[Table]) extends Query
  case class QueryProject(subset:Set[Label], query:Query) extends Query
  case class QueryRestrict(expr:Any /*FIXME*/, query:Query) extends Query
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
  case class QueryOrder(by:Seq[(Label, Order)], query:Query) extends Query
  case class QueryTop(n:Int) extends Query // top n entries


  sealed abstract class Order
  case object Ascending extends Order
  case object Descending extends Order

  //// Expressions

  sealed abstract class Expr
  case class ExprAttributeRef(name:String) extends Expr
  case class ExprConst(value:String /*FIXME?*/) extends Expr
  case class ExprNull(typ:String /*FIXME?*/) extends Expr
  case class ExprApplication(operator:Operator /*FIXME?*/, operands:Seq[String/*FIXME?*/]) extends Expr
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
                         rangeType: Any, /* FIXME (fail, argTypes) => RangeType */
                         proc: Any, /* FIXME Scala implementation of operator (?) */
                         data:Any /* FIXME? domain-specific data for outside use (?)*/ )

  case class CaseBranch(condition:Expr, value:Expr)
}