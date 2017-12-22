package de.ag.sqala

import de.ag.sqala.AggregationOp.Count
import scala.annotation.tailrec

sealed trait Expression {
  def getType(env: Environment): Type

  def getType(scheme: RelationalScheme): Type = getType(scheme.environment)

  def isAggregate: Boolean
  /** Check whether all attribute refs in an expression
    * that are not inside an application of an aggregate occur in `grouped`.
  */
  def checkGrouped(grouped: Option[Set[String]]): Unit

  /** Return all attribute names that occur in the expression. */
  def attributeNames(): Set[String]

  /** evaluate in context that wants a single value */
  def eval1(group: GroupedResult): Any

  /** evaluate in a context that wants a sequence of values */
  def evalAll(group: GroupedResult): Seq[Any]

  def toSqlExpression : SqlExpression
}

object Expression {
  def makeAttributeRef(name: String): Expression = AttributeRef(name)
  def makeConst(ty: Type, value: Any): Expression = Const(ty, value)
  def makeNull(ty: Type): Expression = Null(ty)
  def makeApplication(rator: Rator, rands: Expression*): Expression =
    Application(rator, rands)
  def makeTuple(exprs: Expression*): Expression = Tuple(exprs)
  def makeAggregation(op: AggregationOp, exp: Expression): Expression =
    Aggregation(op, exp)
  def aggregationCountAll: Expression = AggregationAll(AggregationAllOp.CountAll)
  def makeCase(alist: Seq[(Expression, Expression)], default: Expression): Expression =
    Case(alist, default)
  def makeScalarSubquery(query: Query): Expression = ScalarSubquery(query)
  def makeSetSubquery(query: Query): Expression = SetSubquery(query)
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

  override def attributeNames(): Set[String] = Set(name)

  override def eval1(group: GroupedResult): Any = {
    group.lookup(name) match {
      case Left(v) => v
      case Right(vs) => {
        assert(vs.size == 1)
        vs.head
      }
    }
  }

  override def evalAll(group: GroupedResult): Seq[Any] = {
    group.lookup(name) match {
      case Left(v) => group.ungroupedRows.map { _ => v }
      case Right(s) => s
    }
  }

  override def toSqlExpression : SqlExpression = SqlExpressionColumn(name)
}

case class Const(ty: Type, rawValue: Any) extends Expression {
  val value = ty.coerce(rawValue) match {
    case Some(v) => v
    case None => throw new AssertionError("const of invalid type")
  }
  override def getType(env: Environment): Type = ty
  override def isAggregate = false
  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()
  override def attributeNames(): Set[String] = Set()
  override def eval1(group: GroupedResult): Any = value
  override def evalAll(group: GroupedResult): Seq[Any] =
    group.ungroupedRows.map { _ => value }

  override def toSqlExpression : SqlExpression = SqlExpressionConst(ty, value)
}

case class Null(ty: Type) extends Expression {
  override def getType(env: Environment): Type = ty
  override def isAggregate = false
  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()
  override def attributeNames(): Set[String] = Set()
  override def eval1(group: GroupedResult): Any = null
  override def evalAll(group: GroupedResult): Seq[Any] = 
    group.ungroupedRows.map { _ => null }

  override def toSqlExpression : SqlExpression = SqlExpressionNull
}

case class Rator(name: String, rangeType: Seq[Type] => Type, impl: Seq[Any] => Any) {
  def apply(args: Seq[Any]): Any = impl(args)
  def toSqlSelect: SqlOperator = ??? // TODO Expression.Rator translate to SqlOperator
}

object Rator {
  def apply(name: String, rangeType: Seq[Type] => Type): Rator =
    Rator(name, rangeType, { impl: Seq[Any] => ??? })
}

case class Application(rator: Rator, rands: Seq[Expression]) extends Expression {
  override def getType(env: Environment): Type = rator.rangeType(rands.map(_.getType(env)))
  override def isAggregate = false
  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()
  override def attributeNames(): Set[String] =
    rands.flatMap(_.attributeNames()).toSet
  override def eval1(group: GroupedResult): Any =
    rator.apply(rands.map(_.eval1(group)))
  override def evalAll(group: GroupedResult): Seq[Any] = {
    val cols = rands.map { e => e.evalAll(group) }
    cols.transpose.map { row =>
      rator.apply(row)
    }
  }

  override def toSqlExpression : SqlExpression = SqlExpressionApp(rator.toSqlSelect, rands.map(e => e.toSqlExpression))
}

case class Tuple(exprs: Seq[Expression]) extends Expression {
  override def getType(env: Environment): Type =
    Type.product(exprs.map(_.getType(env)))

  override def checkGrouped(grouped: Option[Set[String]]): Unit =
    for (e <- exprs)
      e.checkGrouped(grouped)

  override def isAggregate: Boolean = false

  override def attributeNames(): Set[String] =
    exprs.flatMap(_.attributeNames()).toSet

  override def eval1(group: GroupedResult): Any =
    exprs.map(_.eval1(group)).toArray[Any]

  override def evalAll(group: GroupedResult): Seq[Any] =
    exprs.map { e => e.evalAll(group) }.transpose

  override def toSqlExpression : SqlExpression = SqlExpressionTuple(exprs.map(e => e.toSqlExpression))
}

sealed trait AggregationOp {
  def eval(group: GroupedResult, exp: Expression): Any
}

object AggregationOp {
  case object Count extends AggregationOp {
    def eval(group: GroupedResult, exp: Expression): Any = group.ungroupedRows.length
  }

  case object Sum extends AggregationOp {
    def eval(group: GroupedResult, exp: Expression): Any = {
      exp.getType(group.scheme.environment) match {
        // FIXME: does this work for Int?
        case Type.integer => exp.evalAll(group).map(_.asInstanceOf[Long]).sum
        case Type.double => exp.evalAll(group).map(_.asInstanceOf[Double]).sum 
        case _ => throw new AssertionError("invalid type for sum aggregation")
      }
    }
  }

  def average(sq: Seq[Double]): Double = sq.sum / sq.length

  case object Avg extends AggregationOp {
    def eval(group: GroupedResult, exp: Expression): Any =
      exp.getType(group.scheme) match {
        case Type.double => average(exp.evalAll(group).map(_.asInstanceOf[Double]))
        case _ => throw new AssertionError("invalid type for avg aggregation")
      }
  }
  case object Min extends AggregationOp {
    def eval(group: GroupedResult, exp: Expression): Any =
      exp.getType(group.scheme) match {
        case Type.integer => exp.evalAll(group).map(_.asInstanceOf[Long]).min
        case Type.double => exp.evalAll(group).map(_.asInstanceOf[Double]).min
        case _ => throw new AssertionError("invalid type for min aggregation")
      }
  }
  case object Max extends AggregationOp {
    def eval(group: GroupedResult, exp: Expression): Any =
      exp.getType(group.scheme) match {
        case Type.integer => exp.evalAll(group).map(_.asInstanceOf[Long]).max
        case Type.double => exp.evalAll(group).map(_.asInstanceOf[Double]).max
        case _ => throw new AssertionError("invalid type for max aggregation")
      }
  }

  def standardDeviation(sq: Seq[Double]): Double =
    Math.sqrt(variance(sq))

  case object StdDev extends AggregationOp {
    def eval(group: GroupedResult, exp: Expression): Any =
      exp.getType(group.scheme) match {
        case Type.double => standardDeviation(exp.evalAll(group).map(_.asInstanceOf[Double]))
        case _ => throw new AssertionError("invalid type for avg aggregation")
      }
  }

  def standardDeviationPopulation(sq: Seq[Double]): Double =
    Math.sqrt(variancePopulation(sq))

  case object StdDevP extends AggregationOp {
    def eval(group: GroupedResult, exp: Expression): Any =
      exp.getType(group.scheme) match {
        case Type.double => standardDeviationPopulation(exp.evalAll(group).map(_.asInstanceOf[Double]))
        case _ => throw new AssertionError("invalid type for avg aggregation")
      }
  }

  def variance(sq: Seq[Double]): Double = {
    val avg = average(sq)
    val diffSqrs = sq.map { n => {
      val x = n - avg
      x * x
    }}
    average(diffSqrs)
  }

  case object Var extends AggregationOp {
    def eval(group: GroupedResult, exp: Expression): Any =
      exp.getType(group.scheme) match {
        case Type.double => variance(exp.evalAll(group).map(_.asInstanceOf[Double]))
        case _ => throw new AssertionError("invalid type for avg aggregation")
      }
  }

  def variancePopulation(sq: Seq[Double]): Double = {
    val avg = average(sq)
    val diffSqrs = sq.map { n => {
      val x = n - avg
      x * x
    }}
    diffSqrs.sum / (sq.length - 1)
  }

  case object VarP extends AggregationOp {
    def eval(group: GroupedResult, exp: Expression): Any =
      exp.getType(group.scheme) match {
        case Type.double => variancePopulation(exp.evalAll(group).map(_.asInstanceOf[Double]))
        case _ => throw new AssertionError("invalid type for avg aggregation")
      }
  }
}

case class Aggregation(op: AggregationOp, exp: Expression) extends Expression {

  import AggregationOp._

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

  override def attributeNames(): Set[String] = exp.attributeNames()

  override def eval1(group: GroupedResult): Any = op.eval(group, exp)

  override def evalAll(group: GroupedResult): Seq[Any] = {
    val v = eval1(group)
    group.ungroupedRows.map { _ => v }
  }

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

sealed trait AggregationAllOp {
  def eval(group: GroupedResult): Any
}

object AggregationAllOp {
  case object CountAll extends AggregationAllOp {
    def eval(group: GroupedResult): Any = group.ungroupedRows.size
  }
}

case class AggregationAll(op: AggregationAllOp) extends Expression {
  override def getType(env: Environment): Type = Type.integer

  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()

  override def isAggregate: Boolean = true

  // FIXME: does this get us to empty tuples at some point?
  override def attributeNames(): Set[String] = Set()

  override def eval1(group: GroupedResult): Any = op.eval(group)

  override def evalAll(group: GroupedResult): Seq[Any] = {
    val v = eval1(group)
    group.ungroupedRows.map { _ => v }
  }

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

  override def attributeNames(): Set[String] =
    alist.flatMap { case (te, e) => te.attributeNames().union(e.attributeNames()) }
      .toSet.union(default.attributeNames())

  override def eval1(group: GroupedResult): Any = {
    val rest = alist.dropWhile { case (test, _) => !test.eval1(group).asInstanceOf[Boolean] }
    if (rest.isEmpty)
      default.eval1(group)
    else
      rest.head._2.eval1(group)
  }

  override def evalAll(group: GroupedResult): Seq[Any] = {
    // this should possibly be lazy
    val branches = alist.map { case (test, exp) =>
      (test.evalAll(group).map(_.asInstanceOf[Boolean]),
        exp.evalAll(group))
    }
    val defaultRes = default.evalAll(group)
    (0 until group.ungroupedRows.size).map { i =>
      val rest = branches.dropWhile { case (ts, es) => !ts(i) }
      if (rest.isEmpty)
        defaultRes(i)
      else
        rest.head._2
    }
  }

  override def toSqlExpression : SqlExpression =
    SqlExpressionCase(None,
      alist.map({case (e1, e2) => (e1.toSqlExpression, e2.toSqlExpression)}),
      Some(default.toSqlExpression))
}

case class ScalarSubquery(query: Query) extends Expression {
  override def getType(env: Environment): Type = {
    val scheme = query.getScheme(env)
    if (!scheme.isUnary())
      throw new AssertionError("subquery must have unary scheme")
    scheme.map(scheme.columns.head).toNullable()
  }

  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()

  override def isAggregate: Boolean = false

  override def attributeNames(): Set[String] =
    query.attributeNames()

  override def eval1(group: GroupedResult): Any = {
    val rs = MemoryQuery.computeQueryResults(group, query)
    if (rs.isEmpty)
      null
    else
      rs.head.col0
  }

  override def evalAll(gr: GroupedResult): Seq[Any] =
    gr.oneByOne().map(eval1(_))

  override def toSqlExpression : SqlExpression =
    SqlExpressionSubquery(query.toSqlSelect)
}

case class SetSubquery(query: Query) extends Expression {
  override def getType(env: Environment): Type = {
    val scheme = query.getScheme(env)
    if (!scheme.isUnary())
      throw new AssertionError("subquery must have unary scheme")
    Type.set(scheme.map(scheme.columns.head))
  }

  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()

  override def isAggregate: Boolean = false

  override def attributeNames(): Set[String] =
    query.attributeNames()

  override def eval1(group: GroupedResult): Any =
    throw new AssertionError(s"expected single value, but got set subquery")

  override def evalAll(group: GroupedResult): Seq[Any] =
    MemoryQuery.computeQueryResults(group, query).map(_.col0)

  override def toSqlExpression : SqlExpression = SqlExpressionSubquery(query.toSqlSelect)
}

