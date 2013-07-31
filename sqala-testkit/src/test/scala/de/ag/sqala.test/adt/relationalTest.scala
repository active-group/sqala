package de.ag.sqala.test.adt

import org.scalatest.FunSuite
import de.ag.sqala.relational._
import de.ag.sqala.{Ascending, Operator, Domain}
import de.ag.sqala.DomainChecker.DomainCheckException

/**
 *
 */

class relationalTest extends FunSuite {
  val tbl1 = Query.Base("tbl1", Schema("one" -> Domain.String, "two" -> Domain.Integer))
  val tbl2 = Query.Base("tbl2", Schema("three" -> Domain.Blob, "four" -> Domain.String))

  test("trivial") {
    val q = Query.Project(Seq(
      "two" -> Expr.AttributeRef("two"),
      "one" -> Expr.AttributeRef("one")),
      tbl1)
    expectResult(Schema("two" -> Domain.Integer, "one" -> Domain.String)) {
      q.checkedSchema()
    }
  }

  test("trivial-checkDomain") {
    val q1 = Query.Project(Seq(
      "eq" -> Expr.Application(Operator.Eq, Seq(Expr.AttributeRef("two"), Expr.AttributeRef("two")))
    ),
      tbl1)
    q1.checkDomains() // should not throw DomainCheckException (or any other, that is)
    info("checked domains of q1 schema")
    val q2 = Query.Project(Seq(
      "eq" -> Expr.Application(Operator.Eq, Seq(Expr.AttributeRef("one"), Expr.AttributeRef("two")))
    ),
      tbl1)
    intercept[DomainCheckException]{q2.checkDomains()}
    info("checked domains of q2 schema")
    intercept[DomainCheckException]{Query.Intersection(tbl1, tbl2).checkDomains()}
    info("checked domains of intersection schema")
    intercept[DomainCheckException]{Query.Product(tbl1, tbl1).checkDomains()}
    info("checked domains of product schema")
  }

  test("product") {
    val q1 = Query.Product(tbl1, tbl2)
    val q1Schema = Schema("one" -> Domain.String,
      "two" -> Domain.Integer,
      "three" -> Domain.Blob,
      "four" -> Domain.String)
    expectResult(q1Schema) { q1.checkedSchema() }
  }

  test("case") {
    val q1 = Query.Project(Seq("foo" ->
      Expr.Case(Seq(
        Expr.CaseBranch(Expr.Application(Operator.Eq, Seq(Expr.AttributeRef("two"), Expr.AttributeRef("two"))),
          Expr.AttributeRef("one"))),
        None)),
      tbl1)
    val q2 = Query.Project(Seq("foo" ->
      Expr.Case(Seq(Expr.CaseBranch(Expr.Application(Operator.Eq, Seq(Expr.AttributeRef("two"), Expr.AttributeRef("two"))),
        Expr.AttributeRef("one"))),
        Some(Expr.AttributeRef("one")))),
      tbl1)
    val q3 = Query.Project(Seq("foo" ->
      Expr.Case(Seq(Expr.CaseBranch(Expr.Application(Operator.Eq, Seq(Expr.AttributeRef("two"), Expr.AttributeRef("two"))),
        Expr.AttributeRef("one"))),
        Some(Expr.AttributeRef("two")))),
      tbl1)
    val q1Schema = Schema("foo" -> Domain.String)
    expectResult(q1Schema) { q1.checkedSchema()}
    expectResult(q1Schema) { q2.checkedSchema()}
    intercept[DomainCheckException] { q3.checkDomains()}
  }

  test("ordered") {
    val q1 = Query.Order(Seq(Expr.AttributeRef("one") -> Ascending), tbl1)
    expectResult(tbl1.baseSchema) { q1.checkedSchema() }
    intercept[NoSuchElementException] {
      Query.Order(Seq(Expr.AttributeRef("three") -> Ascending), tbl1).checkDomains()
    }
  }

  test("aggregation") {
    val q1 = Query.Project(Seq("foo" -> Expr.AttributeRef("foo")),
      Query.GroupingProject(Seq("one" -> Expr.AttributeRef("one"), "foo" -> Expr.Aggregation(Operator.Avg, Expr.AttributeRef("two"))), tbl1))
    val q2 = Query.Project(Seq("foo" -> Expr.AttributeRef("foo")),
      Query.GroupingProject(Seq("two" -> Expr.AttributeRef("two"), "foo" -> Expr.Aggregation(Operator.Count, Expr.AttributeRef("one"))), tbl1))
    expectResult(Schema("foo" -> Domain.Integer)) { q1.checkedSchema() }
    intercept[DomainCheckException]{
      Query.GroupingProject(Seq("two" -> Expr.AttributeRef("two"), "foo" -> Expr.Aggregation(Operator.Avg, Expr.AttributeRef("one"))), tbl1).checkedSchema()
    }
    expectResult(Schema("foo" -> Domain.Integer)) { q2.checkedSchema() }
  }

  test("quotient") {
    expectResult(Schema("a" -> Domain.Integer)) {
      Query.Quotient(
        Query.Project(Seq(
          "a" -> Expr.Null(Domain.Integer),
          "b" -> Expr.Null(Domain.Integer)
        ),
          Query.Empty),
        Query.Project(Seq("b" -> Expr.Null(Domain.Integer)), Query.Empty)
      )
        .checkedSchema()
    }
    expectResult(Schema("a" -> Domain.Integer, "c" -> Domain.Integer)) {
      Query.Quotient(
        Query.Project(Seq(
          "a" -> Expr.Null(Domain.Integer),
          "b" -> Expr.Null(Domain.Integer),
          "c" -> Expr.Null(Domain.Integer)
        ),
          Query.Empty
        ),
        Query.Project(Seq("b" -> Expr.Null(Domain.Integer)), Query.Empty)
      )
      .checkedSchema()
    }
    intercept[DomainCheckException] {
      Query.Quotient(
        Query.Project(Seq(
          "a" -> Expr.Null(Domain.Integer),
          "b" -> Expr.Null(Domain.Integer)
        ),
          Query.Empty),
        Query.Project(Seq(
          "a" -> Expr.Null(Domain.Integer),
          "c" -> Expr.Null(Domain.Integer)
        ),
          Query.Empty)
      )
      .checkedSchema()
    }
  }

  test("scalar sub-query") {
    val SUBA = Query.Base("SUBA", Schema("C" -> Domain.String))
    val SUBB = Query.Base("SUBB", Schema("C" -> Domain.String))
    expectResult(Schema("C" -> Domain.String)) {
      Query.Project(Seq("C" -> Expr.AttributeRef("C")),
        Query.Restrict(
          Expr.Application(
            Operator.Eq, Seq(Expr.ScalarSubQuery(Query.Project(Seq("C" -> Expr.AttributeRef("C")),
              SUBB)),
              Expr.AttributeRef("C"))),
          SUBA))
        .checkedSchema()
    }
  }

  test("set sub-query") {
    expectResult(Schema("S" -> Domain.Set(Domain.Integer))) {
      Query.Project(Seq("S" -> Expr.SetSubQuery(
        Query.Project(Seq("two" -> Expr.AttributeRef("two")), tbl1))), Query.Empty)
        .checkedSchema()
    }
  }
}
