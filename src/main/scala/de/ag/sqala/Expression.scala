package de.ag.sqala

import Aliases._
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

  /** evaluate expression */
  def eval(ge: Expression.GroupEnvironment): Any

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


  /** A group environment is used for aggregation: An expression like
    * sum(COL) needs access to not just one value environment, but a
    * value environment for every row in the group.
    * 
    * The scheme field is for figuring out the types of the columns.
    */
  case class GroupEnvironment(scheme: RelationalScheme, rows: Seq[IndexedSeq[Any]]) {
    def lookup(name: String): Any =
      rows.head(scheme.pos(name))

    def count() = rows.size

    def aggregateEval(expr: Expression): Seq[Any] =
      rows.map { e => expr.eval(GroupEnvironment.make(scheme, e)) }

    def compose(other: GroupEnvironment): GroupEnvironment = {
      val newRows = rows.flatMap { row =>
        other.rows.map(row ++ _)
      }
      GroupEnvironment(scheme ++ other.scheme, newRows)
    }
  }

  object GroupEnvironment {
    def make(scheme: RelationalScheme, row: IndexedSeq[Any]): GroupEnvironment =
      GroupEnvironment(scheme, Seq(row))

    def make(name: String, ty: Type, value: Any): GroupEnvironment =
      GroupEnvironment(RelationalScheme.make(name, ty), Seq(IndexedSeq[Any](value)))

    val empty = GroupEnvironment.make(RelationalScheme.make(Seq()), Array[Any]())

  }
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

  override def eval(ge: Expression.GroupEnvironment): Any =
    ge.lookup(name)

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
  override def eval(ge: Expression.GroupEnvironment): Any = value
  override def toSqlExpression : SqlExpression = SqlExpressionConst(ty, value)
}

case class Null(ty: Type) extends Expression {
  override def getType(env: Environment): Type = ty
  override def isAggregate = false
  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()
  override def attributeNames(): Set[String] = Set()
  override def eval(ge: Expression.GroupEnvironment): Any = null
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
  override def eval(ge: Expression.GroupEnvironment): Any =
    rator.apply(rands.map(_.eval(ge)))

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

  override def eval(ge: Expression.GroupEnvironment): Any =
    exprs.map(_.eval(ge)).toArray[Any]

  override def toSqlExpression : SqlExpression = SqlExpressionTuple(exprs.map(e => e.toSqlExpression))
}

sealed trait AggregationOp {
  def eval(ge: Expression.GroupEnvironment, exp: Expression): Any
}

object AggregationOp {
  case object Count extends AggregationOp {
    def eval(ge: Expression.GroupEnvironment, exp: Expression): Any = ge.count()
  }

  case object Sum extends AggregationOp {
    def eval(ge: Expression.GroupEnvironment, exp: Expression): Any = {
      exp.getType(ge.scheme.environment) match {
        // FIXME: does this work for Int?
        case Type.integer => ge.aggregateEval(exp).map(_.asInstanceOf[Long]).sum 
        case Type.double => ge.aggregateEval(exp).map(_.asInstanceOf[Double]).sum 
        case _ => throw new AssertionError("invalid type for sum aggregation")
      }
    }
  }

  def average(sq: Seq[Double]): Double = sq.sum / sq.length

  case object Avg extends AggregationOp {
    def eval(ge: Expression.GroupEnvironment, exp: Expression): Any =
      exp.getType(ge.scheme) match {
        case Type.double => average(ge.aggregateEval(exp).map(_.asInstanceOf[Double]))
        case _ => throw new AssertionError("invalid type for avg aggregation")
      }
  }
  case object Min extends AggregationOp {
    def eval(ge: Expression.GroupEnvironment, exp: Expression): Any =
      exp.getType(ge.scheme) match {
        case Type.integer => ge.aggregateEval(exp).map(_.asInstanceOf[Long]).min
        case Type.double => ge.aggregateEval(exp).map(_.asInstanceOf[Double]).min
        case _ => throw new AssertionError("invalid type for min aggregation")
      }
  }
  case object Max extends AggregationOp {
    def eval(ge: Expression.GroupEnvironment, exp: Expression): Any =
      exp.getType(ge.scheme) match {
        case Type.integer => ge.aggregateEval(exp).map(_.asInstanceOf[Long]).max
        case Type.double => ge.aggregateEval(exp).map(_.asInstanceOf[Double]).max
        case _ => throw new AssertionError("invalid type for max aggregation")
      }
  }

  def standardDeviation(sq: Seq[Double]): Double =
    Math.sqrt(variance(sq))

  case object StdDev extends AggregationOp {
    def eval(ge: Expression.GroupEnvironment, exp: Expression): Any =
      exp.getType(ge.scheme) match {
        case Type.double => standardDeviation(ge.aggregateEval(exp).map(_.asInstanceOf[Double]))
        case _ => throw new AssertionError("invalid type for avg aggregation")
      }
  }

  def standardDeviationPopulation(sq: Seq[Double]): Double =
    Math.sqrt(variancePopulation(sq))

  case object StdDevP extends AggregationOp {
    def eval(ge: Expression.GroupEnvironment, exp: Expression): Any =
      exp.getType(ge.scheme) match {
        case Type.double => standardDeviationPopulation(ge.aggregateEval(exp).map(_.asInstanceOf[Double]))
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
    def eval(ge: Expression.GroupEnvironment, exp: Expression): Any =
      exp.getType(ge.scheme) match {
        case Type.double => variance(ge.aggregateEval(exp).map(_.asInstanceOf[Double]))
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
    def eval(ge: Expression.GroupEnvironment, exp: Expression): Any =
      exp.getType(ge.scheme) match {
        case Type.double => variancePopulation(ge.aggregateEval(exp).map(_.asInstanceOf[Double]))
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

  override def eval(ge: Expression.GroupEnvironment): Any = op.eval(ge, exp)

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
  def eval(ge: Expression.GroupEnvironment): Any
}

object AggregationAllOp {
  case object CountAll extends AggregationAllOp {
    def eval(ge: Expression.GroupEnvironment): Any = ge.count()
  }
}

case class AggregationAll(op: AggregationAllOp) extends Expression {
  override def getType(env: Environment): Type = Type.integer

  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()

  override def isAggregate: Boolean = true

  // FIXME: does this get us to empty tuples at some point?
  override def attributeNames(): Set[String] = Set()

  override def eval(ge: Expression.GroupEnvironment): Any = op.eval(ge)

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

  override def eval(ge: Expression.GroupEnvironment): Any = {
    val rest = alist.dropWhile { case (test, _) => !test.eval(ge).asInstanceOf[Boolean] }
    if (rest.isEmpty)
      default.eval(ge)
    else
      rest.head._2.eval(ge)
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
    scheme.map(scheme.columns.head)
  }

  override def checkGrouped(grouped: Option[Set[String]]): Unit = ()

  override def isAggregate: Boolean = false

  override def attributeNames(): Set[String] =
    query.attributeNames()

  override def eval(ge: Expression.GroupEnvironment): Any = {
    val sq = MemoryQuery.computeQueryResults(ge, query)
    sq.head(0)
  }

  override def toSqlExpression : SqlExpression =
    SqlExpressionSubquery(query.toSqlSelect())
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

  override def eval(ge: Expression.GroupEnvironment): Any = {
    val sq = MemoryQuery.computeQueryResults(ge, query)
    sq.map(_(0))
  }

  override def toSqlExpression : SqlExpression = SqlExpressionSubquery(query.toSqlSelect())
}

