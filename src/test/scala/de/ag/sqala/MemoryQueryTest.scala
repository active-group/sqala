package de.ag.scala

import de.ag.sqala._
import de.ag.sqala.MemoryQuery._
import minitest._
import de.ag.sqala.Expression.GroupEnvironment

object MemoryQueryTest extends SimpleTestSuite {

  test("empty") {
    assertEquals(computeQueryResults(GroupEnvironment.empty, Query.empty), Seq())
  }

  val b1 = Galaxy.makeBaseRelation("stuff", RelationalScheme.make(Seq("num" -> Type.integer, "str" -> Type.string)),
    Seq(IndexedSeq(1L, "foo"), IndexedSeq(2L, "bar")))

  test("base") {
    assertEquals(computeQueryResults(GroupEnvironment.empty, b1),
      Seq(IndexedSeq(1L, "foo"), IndexedSeq(2L, "bar")))
  }

  test("projection") {
    val q = b1.project(Seq("num2" -> Expression.makeAttributeRef("num")))
    assertEquals(computeQueryResults(GroupEnvironment.empty, q),
      Seq(IndexedSeq(1L), IndexedSeq(2L)))
  }

  val lessThan = Rator("int<",
    { case Seq(ty1, ty2) =>
      assert(ty1 == Type.integer)
      assert(ty2 == Type.integer)
      Type.boolean },
    { case Seq(arg1, arg2) =>
      arg1.asInstanceOf[Long] < arg2.asInstanceOf[Long]
    })


  test("restriction") {
    val q = b1.restrict(Expression.makeApplication(lessThan,
      Expression.makeAttributeRef("num"), Expression.makeConst(Type.integer, 2L)))
    assertEquals(computeQueryResults(GroupEnvironment.empty, q),
      Seq(IndexedSeq(1L, "foo")))
  }

  val s2 = RelationalScheme.make(Seq("str2" -> Type.string, "double" -> Type.double))
  val sq2 = Seq(IndexedSeq("blaz", 5L), IndexedSeq("bam", 6L), IndexedSeq("buz", 7L))
  val b2 = Galaxy.makeBaseRelation("stuff2", s2, sq2)
  val sq2a = Seq(IndexedSeq("hurz", 23L), IndexedSeq("bam", 6L), IndexedSeq("gluck", 25L))
  val b2a = Galaxy.makeBaseRelation("stuff2a", s2, sq2a)

  test("product") {
    assertEquals(computeQueryResults(b1 * b2),
      Seq(
        IndexedSeq(1L, "foo", "blaz", 5L),
        IndexedSeq(1L, "foo", "bam", 6L),
        IndexedSeq(1L, "foo", "buz", 7L),
        IndexedSeq(2L, "bar", "blaz", 5L),
        IndexedSeq(2L, "bar", "bam", 6L),
        IndexedSeq(2L, "bar", "buz", 7L)))
  }

  test("union") {
    assertEquals(computeQueryResults(b2.union(b2a)).toSet,
      sq2.toSet ++ sq2a.toSet)
  }

  test("intersection") {
    assertEquals(computeQueryResults(GroupEnvironment.empty, b2.intersection(b2a)).toSet,
      sq2.toSet.intersect(sq2a.toSet))
  }

  test("quotient") {
    // example from Elmasri/Navathe
    val r = Galaxy.makeBaseRelation("R",
      RelationalScheme.make(Seq("A" -> Type.string, "B" -> Type.string)),
      Seq(
        IndexedSeq("a1", "b1"),
	IndexedSeq("a2", "b1"),
	IndexedSeq("a3", "b1"),
	IndexedSeq("a4", "b1"),
	IndexedSeq("a1", "b2"),
	IndexedSeq("a3", "b2"),
	IndexedSeq("a2", "b3"),
	IndexedSeq("a3", "b3"),
	IndexedSeq("a4", "b3"),
	IndexedSeq("a1", "b4"),
	IndexedSeq("a2", "b4"),
	IndexedSeq("a3", "b4")))
    val s = Galaxy.makeBaseRelation("S",
      RelationalScheme.make(Seq("A" -> Type.string)),
      Seq(
        IndexedSeq("a1"),
        IndexedSeq("a2"),
        IndexedSeq("a3")))
    assertEquals(computeQueryResults(r / s),
      Seq(IndexedSeq("b1"), IndexedSeq("b4")))
  }
}
