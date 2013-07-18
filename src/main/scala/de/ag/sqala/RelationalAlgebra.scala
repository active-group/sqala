package de.ag.sqala

class Label(val label:String) // FIXME type alias?

class Schema(val schema:Seq[(Label, Domain)]) {
  private lazy val schemaMap = schema.toMap
  def dom(label:Label) = schemaMap(label)
  def degree = schemaMap.size
  def difference(that:Schema):Schema = {
    val thatKeys = that.schemaMap.keySet
    // keep order of labels
    new Schema(schema.filter(ld => !thatKeys.contains(ld._1)))
  }
  def isUnary = degree == 1
  def labels = schema.map(_._1)
  def domains = schema.map(_._2)
}

class RelQueryName(name:String) // FIXME type alias?

class Universe // FIXME

sealed abstract class RelQuery
case object RelQueryEmpty extends RelQuery
case class RelQueryBase(name:RelQueryName,
                        schema:Schema,
                        universe:Option[Universe],
                        table:Option[SqlTable]) extends RelQuery
case class RelQueryProject(subset:Set[Label], query:RelQuery) extends RelQuery
case class RelQueryRestrict(expr:Any /*FIXME*/, query:RelQuery) extends RelQuery
case class RelQueryProduct(query1:RelQuery, query2:RelQuery) extends RelQuery
case class RelQueryUnion(query1:RelQuery, query2:RelQuery) extends RelQuery
case class RelQueryIntersection(query1:RelQuery, query2:RelQuery) extends RelQuery
case class RelQueryQuotient(query1:RelQuery, query2:RelQuery) extends RelQuery
case class RelQueryDifference(query1:RelQuery, query2:RelQuery) extends RelQuery
/*
; the underlying query is grouped by the non-aggregate expressions in
; the alist (hu? FIXME)
*/
case class RelQueryGroupingProject(alist:Any /*FIXME*/, query:RelQuery) extends RelQuery
case class RelQueryOrder(by:Seq[(Label, Order)], query:RelQuery) extends RelQuery
case class RelQueryTop(n:Int) extends RelQuery // top n entries


sealed abstract class Order
case object Ascending extends Order
case object Descending extends Order

//// Expressions

sealed abstract class RelExpr
case class RelExprAttributeRef(name:String) extends RelExpr
case class RelExprConst(value:String /*FIXME?*/) extends RelExpr
case class RelExprNull(typ:String /*FIXME?*/) extends RelExpr
case class RelExprApplication(operator:RelOperator /*FIXME?*/, operands:Seq[String/*FIXME?*/]) extends RelExpr
case class RelExprTuple(expressions:Seq[RelExpr]) extends RelExpr
case class RelExprAggregation(op:Either[AggregationOp, String], expr:RelExpr) extends RelExpr
case class RelExprCase(branches:Seq[CaseBranch], default:Option[RelExpr]) extends RelExpr
case class RelExprScalarSubQuery(query:RelQuery /*FIXME*/) extends RelExpr
case class RelExprSetSubQuery(query:RelQuery /*FIXME*/) extends RelExpr


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

case class RelOperator(name: String,
                       rangeType: Any, /* FIXME (fail, argTypes) => RangeType */
                       proc: Any, /* FIXME Scala implementation of operator (?) */
                       data:Any /* FIXME? domain-specific data for outside use (?)*/ )

case class CaseBranch(condition:RelExpr, value:RelExpr)