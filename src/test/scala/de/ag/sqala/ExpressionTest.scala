package de.ag.scala

import de.ag.sqala._
import minitest._

object ExpressionTest extends SimpleTestSuite {

  def testOnError[A](x: A, proc: A => Any) = {
    try {
      proc(x)
      fail(x+"failed!")
    } catch {
      case _ : AssertionError => // wanted
    }
  }

  val env1 : Aliases.Environment = Map(
    "id" -> Type.integer,
    "name" -> Type.string)

  val env2 : Aliases.Environment = Map(
    "id" -> Type.string,
    "isReal" -> Type.boolean
  )

  val ref1 = Expression.makeAttributeRef("left")
  val ref2 = Expression.makeAttributeRef("id")
  val ref3 = Expression.makeAttributeRef("name")

  val const1 = Expression.makeConst(Type.string, "name")
  val const2 = Expression.makeConst(Type.integer, 5)
  val const3 = Expression.makeConst(Type.boolean, true)


  test("getType") {
    assertEquals(ref2.getType(env1), Type.integer)
    assertEquals(ref2.getType(env2), Type.string)
    assertEquals(ref3.getType(env1), Type.string)
    assertEquals(const1.getType(env1), Type.string)
    assertEquals(const2.getType(env2), Type.integer)
    assertEquals(const3.getType(env1), Type.boolean)

    
    assertEquals(Aggregation(AggregationOp.Count, ref2).getType(env2), Type.integer)
    testOnError[Expression](Aggregation(AggregationOp.Sum, const1), x => x.getType(env1))

    val allIntAggOps : Seq[AggregationOp] = Seq(AggregationOp.Sum, AggregationOp.Avg,
      AggregationOp.StdDev, AggregationOp.StdDevP, AggregationOp.Var, AggregationOp.VarP)
    for (op <- allIntAggOps) {
      assertEquals(Aggregation(op, const2).getType(env1), Type.integer)
      testOnError[Expression](Aggregation(op, const1), x => x.getType(env2))
      testOnError[Expression](Aggregation(op, const3), x => x.getType(env2))
      assertEquals(Aggregation(op, ref2).getType(env1), Type.integer)
      testOnError[Expression](Aggregation(op, ref2), x => x.getType(env2))
    }

    val orderedAggs : Seq[AggregationOp] = Seq(AggregationOp.Max, AggregationOp.Min)
    for (op <- orderedAggs) {
      assertEquals(Aggregation(op, const1).getType(env1), Type.string)
      assertEquals(Aggregation(op, const2).getType(env1), Type.integer)
      testOnError[Expression](Aggregation(op, const3), x => x.getType(env1))
      assertEquals(Aggregation(op, ref2).getType(env1), Type.integer)
      assertEquals(Aggregation(op, ref2).getType(env2), Type.string)
    }

    assertEquals(AggregationAll(AggregationAllOp.CountAll).getType(env1), Type.integer)
    assertEquals(AggregationAll(AggregationAllOp.CountAll).getType(env2), Type.integer)
  }


  test("toSqlExpression") {
    assertEquals(AttributeRef("blub").toSqlExpression, SqlExpressionColumn("blub"))
    assertEquals(Const(Type.integer, 4).toSqlExpression, SqlExpressionConst(Type.integer, 4))
    assertEquals(Null(Type.boolean).toSqlExpression, SqlExpressionNull)
    // ToDo Test Expression.Application
    assertEquals(Tuple(Seq(AttributeRef("a"), Const(Type.string, "gahh"))).toSqlExpression,
      SqlExpressionTuple(Seq(SqlExpressionColumn("a"), SqlExpressionConst(Type.string, "gahh"))))
    assertEquals(Aggregation(AggregationOp.Sum, AttributeRef("anz")).toSqlExpression,
      SqlExpressionApp(SqlOperator.sum, Seq(SqlExpressionColumn("anz"))))
    assertEquals(Case(Seq((Const(Type.integer, 5), AttributeRef("a")), (Const(Type.integer, 8), AttributeRef("b"))),
      AttributeRef("c")).toSqlExpression,
      SqlExpressionCase(None, Seq(
        (SqlExpressionConst(Type.integer, 5), SqlExpressionColumn("a")),
        (SqlExpressionConst(Type.integer, 8), SqlExpressionColumn("b"))), Some(SqlExpressionColumn("c"))))
  }

  test("attributeNames") {
    import de.ag.sqala.Expression._
    assertEquals(makeAttributeRef("blub").attributeNames(), Set("blub"))
    assertEquals(makeConst(Type.integer, 4).attributeNames(), Set())
    assertEquals(makeNull(Type.boolean).attributeNames(), Set())
    assertEquals(makeTuple(makeAttributeRef("a"), makeConst(Type.string, "gahh")).attributeNames(),
      Set("a"))

  }

  test("eval") {
    import de.ag.sqala.Expression._
    assertEquals(makeNull(Type.boolean).eval(GroupEnvironment.empty), null)
    assertEquals(makeConst(Type.boolean, true).eval(GroupEnvironment.empty), true)
    assertEquals(makeAttributeRef("foo").eval(GroupEnvironment.make("foo", Type.boolean, true)), true)
    assertEquals(makeTuple(makeConst(Type.integer, 1), makeConst(Type.integer, 2), makeConst(Type.integer, 3)).eval(GroupEnvironment.empty)
      .asInstanceOf[Array[Any]].deep,
      Array[Any](1, 2, 3).deep)


    val plus = Rator("int+", { _ => Type.integer }, { args => args.map(_.asInstanceOf[Long]).sum })
    assertEquals(makeApplication(plus, makeConst(Type.integer, 23), makeConst(Type.integer, 42)).eval(GroupEnvironment.empty),
      65)

    {
      val scheme = RelationalScheme.make("foo", Type.integer)
      val ge = GroupEnvironment(scheme, Seq(Array(1L),
        Array(1L),
        Array(2L),
        Array(3L),
        Array(5L)))
      assertEquals(makeAggregation(AggregationOp.Count, makeAttributeRef("foo")).eval(ge), 5)
      assertEquals(makeAggregation(AggregationOp.Sum, makeAttributeRef("foo")).eval(ge), 12)
      assertEquals(makeAggregation(AggregationOp.Min, makeAttributeRef("foo")).eval(ge), 1)
      assertEquals(makeAggregation(AggregationOp.Max, makeAttributeRef("foo")).eval(ge), 5)
    }

    {
      val scheme = RelationalScheme.make("foo", Type.double)
      val ge = GroupEnvironment(scheme, Seq(Array(1.0),
        Array(1.0),
        Array(2.0),
        Array(3.0),
        Array(5.0)))
      assertEquals(makeAggregation(AggregationOp.Count, makeAttributeRef("foo")).eval(ge), 5.0)
      assertEquals(makeAggregation(AggregationOp.Sum, makeAttributeRef("foo")).eval(ge), 12.0)
      assertEquals(makeAggregation(AggregationOp.Min, makeAttributeRef("foo")).eval(ge), 1.0)
      assertEquals(makeAggregation(AggregationOp.Max, makeAttributeRef("foo")).eval(ge), 5.0)
      assertEquals(makeAggregation(AggregationOp.Avg, makeAttributeRef("foo")).eval(ge), 2.4)
      assertEquals(makeAggregation(AggregationOp.StdDev, makeAttributeRef("foo")).eval(ge), 1.4966629547095764)
      assertEquals(makeAggregation(AggregationOp.StdDevP, makeAttributeRef("foo")).eval(ge), 1.6733200530681511)
      assertEquals(makeAggregation(AggregationOp.Var, makeAttributeRef("foo")).eval(ge), 2.2399999999999998)
      assertEquals(makeAggregation(AggregationOp.VarP, makeAttributeRef("foo")).eval(ge), 2.8)
    }

    {
      val scheme = RelationalScheme.make("foo", Type.integer)
      val ge = GroupEnvironment(scheme, Seq(Array(1L),
        Array(1L),
        Array(2L),
        Array(3L),
        Array(5L)))
      assertEquals(aggregationCountAll.eval(ge), 5)
    }

    {
      assertEquals(makeCase(Seq(makeConst(Type.boolean, true) -> makeConst(Type.integer, 1),
        makeConst(Type.boolean, false) -> makeConst(Type.integer, 2)),
        makeConst(Type.integer, -1)).eval(GroupEnvironment.empty),
        1)
      assertEquals(makeCase(Seq(makeConst(Type.boolean, false) -> makeConst(Type.integer, 1),
        makeConst(Type.boolean, true) -> makeConst(Type.integer, 2)),
        makeConst(Type.integer, -1)).eval(GroupEnvironment.empty),
        2)
      assertEquals(makeCase(Seq(makeConst(Type.boolean, false) -> makeConst(Type.integer, 1),
        makeConst(Type.boolean, false) -> makeConst(Type.integer, 2)),
        makeConst(Type.integer, -1)).eval(GroupEnvironment.empty),
        -1)
    }

  }


}