package de.ag.sqala.test

import org.scalatest.FunSuite
import de.ag.sqala.relational._
import de.ag.sqala.{Ascending, Operator, Domain}
import de.ag.sqala.DomainChecker.DomainCheckException

/**
 *
 */

class relationalTest extends FunSuite {
  val tbl1 = QueryBase("tbl1", Schema(Seq("one" -> Domain.String, "two" -> Domain.Integer)))
  val tbl2 = QueryBase("tbl2", Schema(Seq("three" -> Domain.Blob, "four" -> Domain.String)))

  test("trivial") {
    val q = QueryProject(Seq(
      "two" -> Expr.AttributeRef("two"),
      "one" -> Expr.AttributeRef("one")),
      tbl1)
    expectResult(Schema(Seq("two" -> Domain.Integer, "one" -> Domain.String))) {
      q.checkedSchema()
    }
  }

  test("trivial-checkDomain") {
    val q1 = QueryProject(Seq(
      "eq" -> Expr.Application(Operator.Eq, Seq(Expr.AttributeRef("two"), Expr.AttributeRef("two")))
    ),
      tbl1)
    q1.checkDomains() // should not throw DomainCheckException (or any other, that is)
    info("checked domains of q1 schema")
    val q2 = QueryProject(Seq(
      "eq" -> Expr.Application(Operator.Eq, Seq(Expr.AttributeRef("one"), Expr.AttributeRef("two")))
    ),
      tbl1)
    intercept[DomainCheckException]{q2.checkDomains()}
    info("checked domains of q2 schema")
    intercept[DomainCheckException]{QueryIntersection(tbl1, tbl2).checkDomains()}
    info("checked domains of intersection schema")
    intercept[DomainCheckException]{QueryProduct(tbl1, tbl1).checkDomains()}
    info("checked domains of product schema")
  }

  test("product") {
    val q1 = QueryProduct(tbl1, tbl2)
    val q1Schema = Schema(Seq("one" -> Domain.String,
      "two" -> Domain.Integer,
      "three" -> Domain.Blob,
      "four" -> Domain.String))
    expectResult(q1Schema) { q1.checkedSchema() }
  }

  test("case") {
    val q1 = QueryProject(Seq("foo" ->
      Expr.Case(Seq(
        Expr.CaseBranch(Expr.Application(Operator.Eq, Seq(Expr.AttributeRef("two"), Expr.AttributeRef("two"))),
          Expr.AttributeRef("one"))),
        None)),
      tbl1)
    val q2 = QueryProject(Seq("foo" ->
      Expr.Case(Seq(Expr.CaseBranch(Expr.Application(Operator.Eq, Seq(Expr.AttributeRef("two"), Expr.AttributeRef("two"))),
        Expr.AttributeRef("one"))),
        Some(Expr.AttributeRef("one")))),
      tbl1)
    val q3 = QueryProject(Seq("foo" ->
      Expr.Case(Seq(Expr.CaseBranch(Expr.Application(Operator.Eq, Seq(Expr.AttributeRef("two"), Expr.AttributeRef("two"))),
        Expr.AttributeRef("one"))),
        Some(Expr.AttributeRef("two")))),
      tbl1)
    val q1Schema = Schema(Seq("foo" -> Domain.String))
    expectResult(q1Schema) { q1.checkedSchema()}
    expectResult(q1Schema) { q2.checkedSchema()}
    intercept[DomainCheckException] { q3.checkDomains()}
  }

  test("ordered") {
    val q1 = QueryOrder(Seq(Expr.AttributeRef("one") -> Ascending), tbl1)
    expectResult(tbl1.base) { q1.checkedSchema() }
    intercept[NoSuchElementException] {
      QueryOrder(Seq(Expr.AttributeRef("three") -> Ascending), tbl1).checkDomains()
    }
  }

  test("aggregation") {
    val q1 = QueryProject(Seq("foo" -> Expr.AttributeRef("foo")),
      QueryGroupingProject(Seq("one" -> Expr.AttributeRef("one"), "foo" -> Expr.Aggregation(Operator.Avg, Expr.AttributeRef("two"))), tbl1))
    val q2 = QueryProject(Seq("foo" -> Expr.AttributeRef("foo")),
      QueryGroupingProject(Seq("two" -> Expr.AttributeRef("two"), "foo" -> Expr.Aggregation(Operator.Count, Expr.AttributeRef("one"))), tbl1))
    expectResult(Schema(Seq("foo" -> Domain.Integer))) { q1.checkedSchema() }
    intercept[DomainCheckException]{
      QueryGroupingProject(Seq("two" -> Expr.AttributeRef("two"), "foo" -> Expr.Aggregation(Operator.Avg, Expr.AttributeRef("one"))), tbl1).checkedSchema()
    }
    expectResult(Schema(Seq("foo" -> Domain.Integer))) { q2.checkedSchema() }
  }

  test("quotient") {
    expectResult(Schema(Seq("a" -> Domain.Integer))) {
      QueryQuotient(
        QueryProject(Seq(
          "a" -> Expr.Null(Domain.Integer),
          "b" -> Expr.Null(Domain.Integer)
        ),
          QueryEmpty),
        QueryProject(Seq("b" -> Expr.Null(Domain.Integer)), QueryEmpty)
      )
        .checkedSchema()
    }
    expectResult(Schema(Seq("a" -> Domain.Integer, "c" -> Domain.Integer))) {
      QueryQuotient(
        QueryProject(Seq(
          "a" -> Expr.Null(Domain.Integer),
          "b" -> Expr.Null(Domain.Integer),
          "c" -> Expr.Null(Domain.Integer)
        ),
          QueryEmpty
        ),
        QueryProject(Seq("b" -> Expr.Null(Domain.Integer)), QueryEmpty)
      )
      .checkedSchema()
    }
    intercept[DomainCheckException] {
      QueryQuotient(
        QueryProject(Seq(
          "a" -> Expr.Null(Domain.Integer),
          "b" -> Expr.Null(Domain.Integer)
        ),
          QueryEmpty),
        QueryProject(Seq(
          "a" -> Expr.Null(Domain.Integer),
          "c" -> Expr.Null(Domain.Integer)
        ),
          QueryEmpty)
      )
      .checkedSchema()
    }
  }

  test("scalar sub-query") {
    val SUBA = QueryBase("SUBA", Schema(Seq("C" -> Domain.String)))
    val SUBB = QueryBase("SUBB", Schema(Seq("C" -> Domain.String)))
    expectResult(Schema(Seq("C" -> Domain.String))) {
      QueryProject(Seq("C" -> Expr.AttributeRef("C")),
        QueryRestrict(
          Expr.Application(
            Operator.Eq, Seq(Expr.ScalarSubQuery(QueryProject(Seq("C" -> Expr.AttributeRef("C")),
              SUBB)),
              Expr.AttributeRef("C"))),
          SUBA))
        .checkedSchema()
    }
  }

  test("set sub-query") {
    expectResult(Schema(Seq("S" -> Domain.Set(Domain.Integer)))) {
      QueryProject(Seq("S" -> Expr.SetSubQuery(
        QueryProject(Seq("two" -> Expr.AttributeRef("two")), tbl1))), QueryEmpty)
        .checkedSchema()
    }
  }
}
