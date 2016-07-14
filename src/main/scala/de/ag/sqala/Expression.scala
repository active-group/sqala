package de.ag.sqala

import Aliases._

sealed trait Expression {
  def getType(env: Environment): Type
  def isAggregate: Boolean
  /** Check whether all attribute refs in an expression
    * that are not inside an application of an aggregate occur in `grouped`.
  */
  def checkGrouped(grouped: Option[Set[String]]): Unit
}

object Expression {
  def makeAttributeRef(name: String): Expression = AttributeRef(name)
  def makeConst(ty: Type, value: Any): Expression = Const(ty, value)
}

case class AttributeRef(val name: String) extends Expression {
  def getType(env: Environment): Type = env(name)
  def isAggregate = false
  def checkGrouped(grouped: Option[Set[String]]): Unit = {
    grouped match {
      case None => throw new AssertionError("non-aggregate expression")
      case Some(s) => assert(s.contains(name))
    }
    ()
  }
}

case class Const(val ty: Type, val value: Any) extends Expression {
  def getType(env: Environment): Type = ty
  def isAggregate = false
  def checkGrouped(grouped: Option[Set[String]]): Unit = ()
}

sealed trait AggregationOp

object AggregationOp {
  case object Count extends AggregationOp
  case object Sum extends AggregationOp
  case object Avg extends AggregationOp
  case object Min extends AggregationOp
  case object Max extends AggregationOp
  case object StdDev extends AggregationOp
  case object StdDevP extends AggregationOp
  case object Var extends AggregationOp
  case object VarP extends AggregationOp
}

case class Aggregation(op: AggregationOp, exp: Expression) extends Expression {
  override def getType(env: Environment): Type = {
    import AggregationOp._
    val t = exp.getType(env)
    op match {
      case Count => Type.integer
      case Sum | Avg | StdDev | StdDevP | Var | VarP =>
        if (t.isNumeric)
          t
        else
          throw new AssertionError("aggregation expression does not have a numeric type in " + this)
      case  Min | Max =>
        if (t.isOrdered)
          t
        else
          throw new AssertionError("aggregation expression does not have an ordered type in " + this)
    }
  }

  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()

  override def isAggregate: Boolean = true
}

sealed trait AggregationAllOp

object AggregationAllOp {
  case object CountAll extends AggregationAllOp
}

case class AggregationAll(op: AggregationAllOp) extends Expression {
  override def getType(env: Environment): Type = Type.integer

  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()

  override def isAggregate: Boolean = true
}
