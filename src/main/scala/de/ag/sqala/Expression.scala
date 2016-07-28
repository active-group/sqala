package de.ag.sqala

import Aliases._
import de.ag.sqala.AggregationOp.Count

sealed trait Expression {
  def getType(env: Environment): Type
  def isAggregate: Boolean
  /** Check whether all attribute refs in an expression
    * that are not inside an application of an aggregate occur in `grouped`.
  */
  def checkGrouped(grouped: Option[Set[String]]): Unit

  def toSqlExpression : SqlExpression
}

object Expression {
  def makeAttributeRef(name: String): Expression = AttributeRef(name)
  def makeConst(ty: Type, value: Any): Expression = Const(ty, value)
}

case class AttributeRef(name: String) extends Expression {
  override def getType(env: Environment): Type = env(name)
  override def isAggregate = false
  override def checkGrouped(grouped: Option[Set[String]]): Unit = {
    grouped match {
      case None => throw new AssertionError("non-aggregate expression")
      case Some(s) => assert(s.contains(name))
    }
    ()
  }
  override def toSqlExpression : SqlExpression = SqlExpressionColumn(name)
}

case class Const(ty: Type, value: Any) extends Expression {
  override def getType(env: Environment): Type = ty
  override def isAggregate = false
  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()
  override def toSqlExpression : SqlExpression = SqlExpressionConst(ty, value)
}

case class Null(ty: Type) extends Expression {
  override def getType(env: Environment): Type = ty
  override def isAggregate = false
  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()
  override def toSqlExpression : SqlExpression = SqlExpressionNull
}

case class Rator(name: String, rangeType: Seq[Type] => Type) {
  def toSqlSelect: SqlOperator = ??? // TODO Expression.Rator translate to SqlOperator
}

case class Application(rator: Rator, rands: Seq[Expression]) extends Expression {
  override def getType(env: Environment): Type = rator.rangeType(rands.map(_.getType(env)))
  override def isAggregate = false
  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()
  override def toSqlExpression : SqlExpression = SqlExpressionApp(rator.toSqlSelect, rands.map(e => e.toSqlExpression))
}

case class Tuple(exprs: Seq[Expression]) extends Expression {
  override def getType(env: Environment): Type =
    Type.product(exprs.map(_.getType(env)))

  override def checkGrouped(grouped: Option[Set[String]]): Unit =
    for (e <- exprs)
      e.checkGrouped(grouped)

  override def isAggregate: Boolean = false

  override def toSqlExpression : SqlExpression = SqlExpressionTuple(exprs.map(e => e.toSqlExpression))
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

  override def toSqlExpression : SqlExpression = SqlExpressionApp(op match {
    case AggregationOp.Count   => SqlOperator.count
    case AggregationOp.Sum     => SqlOperator.sum
    case AggregationOp.Avg     => SqlOperator.avg
    case AggregationOp.Min     => SqlOperator.min
    case AggregationOp.Max     => SqlOperator.max
    case AggregationOp.StdDev  => SqlOperator.stdDev
    case AggregationOp.StdDevP => SqlOperator.stdDevP
    case AggregationOp.Var     => SqlOperator.vari
    case AggregationOp.VarP    => SqlOperator.varP
  }, Seq(exp.toSqlExpression))
}

sealed trait AggregationAllOp

object AggregationAllOp {
  case object CountAll extends AggregationAllOp
}

case class AggregationAll(op: AggregationAllOp) extends Expression {
  override def getType(env: Environment): Type = Type.integer

  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()

  override def isAggregate: Boolean = true

  override def toSqlExpression : SqlExpression = SqlExpressionApp(op match {
    case AggregationAllOp.CountAll => SqlOperator.countAll
  }, Seq())
}

case class Case(alist: Seq[(Expression, Expression)], default: Expression) extends Expression {
  override def getType(env: Environment): Type = {
    val t = default.getType(env)
    for ((te, e) <- alist) {
      if (te.getType(env) != Type.boolean)
        throw new AssertionError("non-boolean test in case")
      if (e.getType(env) != t)
        throw new AssertionError("type mismatch in case")
    }
    t
  }

  override def checkGrouped(grouped: Option[Set[String]]): Unit = {
    for ((te, e) <- alist) {
      te.checkGrouped(grouped)
      e.checkGrouped(grouped)
    }
    default.checkGrouped(grouped)
  }

  override def isAggregate: Boolean = false

  override def toSqlExpression : SqlExpression = SqlExpressionCase(None, alist.map({case (e1, e2) => (e1.toSqlExpression, e2.toSqlExpression)}), Some(default.toSqlExpression))
}

case class ScalarSubquery(query: Query) extends Expression {
  override def getType(env: Environment): Type = {
    val scheme = query.getScheme(env)
    if (!scheme.isUnary())
      throw new AssertionError("subquery mus have unary scheme")
    scheme.map(scheme.columns.head)
  }

  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()

  override def isAggregate: Boolean = false

  override def toSqlExpression : SqlExpression = SqlExpressionSubquery(query.toSqlSelect())
}

case class SetSubquery(query: Query) extends Expression {
  override def getType(env: Environment): Type = {
    val scheme = query.getScheme(env)
    if (!scheme.isUnary())
      throw new AssertionError("subquery mus have unary scheme")
    Type.set(scheme.map(scheme.columns.head))
  }

  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()

  override def isAggregate: Boolean = false

  override def toSqlExpression : SqlExpression = SqlExpressionSubquery(query.toSqlSelect())
}