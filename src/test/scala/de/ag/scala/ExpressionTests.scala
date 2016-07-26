package de.ag.scala

import de.ag.sqala._
import minitest._

object ExpressionTests extends SimpleTestSuite {

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
  val const2 = Expression.makeConst(Type.integer, "id")
  val const3 = Expression.makeConst(Type.boolean, "isHere")


  test("getType") {
    assertEquals(ref2.getType(env1), Type.integer)
    assertEquals(ref2.getType(env2), Type.string)
    assertEquals(ref3.getType(env1), Type.string)
    assertEquals(const1.getType(env1), Type.string)
    assertEquals(const2.getType(env2), Type.integer)
    assertEquals(const3.getType(env1), Type.boolean)

    assertEquals(Aggregation(AggregationOp.Count, ref2).getType(env2), Type.integer)
    testOnError[Expression](Aggregation(AggregationOp.Sum, const1), x => x.getType(env1))

    def checkOnBothEnvsForIntAgg(aggOp: AggregationOp) = {
    }
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


}
