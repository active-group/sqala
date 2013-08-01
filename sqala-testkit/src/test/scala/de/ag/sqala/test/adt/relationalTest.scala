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
    val q = Query.Project(tbl1,
      "two" -> Expr.AttributeRef("two"),
      "one" -> Expr.AttributeRef("one"))
    expectResult(Schema("two" -> Domain.Integer, "one" -> Domain.String)) {
      q.checkedSchema()
    }
  }

  test("trivial-checkDomain") {
    val q1 = Query.Project(tbl1,
      "eq" -> Expr.Application(Operator.Eq, Expr.AttributeRef("two"), Expr.AttributeRef("two")))
    q1.checkDomains() // should not throw DomainCheckException (or any other, that is)
    info("checked domains of q1 schema")
    val q2 = Query.Project(tbl1,
      "eq" -> Expr.Application(Operator.Eq, Expr.AttributeRef("one"), Expr.AttributeRef("two")))
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
    val q1 = Query.Project(tbl1, "foo" ->
      Expr.Case(Seq(
        Expr.CaseBranch(Expr.Application(Operator.Eq, Expr.AttributeRef("two"), Expr.AttributeRef("two")),
          Expr.AttributeRef("one"))),
        None))
    val q2 = Query.Project(tbl1, "foo" ->
      Expr.Case(Seq(Expr.CaseBranch(Expr.Application(Operator.Eq, Expr.AttributeRef("two"), Expr.AttributeRef("two")),
        Expr.AttributeRef("one"))),
        Some(Expr.AttributeRef("one"))))
    val q3 = Query.Project(tbl1, "foo" ->
      Expr.Case(Seq(Expr.CaseBranch(Expr.Application(Operator.Eq, Expr.AttributeRef("two"), Expr.AttributeRef("two")),
        Expr.AttributeRef("one"))),
        Some(Expr.AttributeRef("two"))))
    val q1Schema = Schema("foo" -> Domain.String)
    expectResult(q1Schema) { q1.checkedSchema()}
    expectResult(q1Schema) { q2.checkedSchema()}
    intercept[DomainCheckException] { q3.checkDomains()}
  }

  test("ordered") {
    val q1 = Query.Order(tbl1, Expr.AttributeRef("one") -> Ascending)
    expectResult(tbl1.baseSchema) { q1.checkedSchema() }
    intercept[NoSuchElementException] {
      Query.Order(tbl1, Expr.AttributeRef("three") -> Ascending).checkDomains()
    }
  }

  test("aggregation") {
    val q1 = Query.Project(Query.GroupingProject(tbl1, "one" -> Expr.AttributeRef("one"), "foo" -> Expr.Aggregation(Operator.Avg, Expr.AttributeRef("two"))),
      "foo" -> Expr.AttributeRef("foo"))
    val q2 = Query.Project(Query.GroupingProject(tbl1, "two" -> Expr.AttributeRef("two"), "foo" -> Expr.Aggregation(Operator.Count, Expr.AttributeRef("one"))),
      "foo" -> Expr.AttributeRef("foo"))
    expectResult(Schema("foo" -> Domain.Integer)) { q1.checkedSchema() }
    intercept[DomainCheckException]{
      Query.GroupingProject(tbl1, "two" -> Expr.AttributeRef("two"), "foo" -> Expr.Aggregation(Operator.Avg, Expr.AttributeRef("one"))).checkedSchema()
    }
    expectResult(Schema("foo" -> Domain.Integer)) { q2.checkedSchema() }
  }

  test("quotient") {
    expectResult(Schema("a" -> Domain.Integer)) {
      Query.Quotient(
        Query.Project(Query.Empty,
          "a" -> Expr.Null(Domain.Integer),
          "b" -> Expr.Null(Domain.Integer)
        ),
        Query.Project(Query.Empty, "b" -> Expr.Null(Domain.Integer))
      )
        .checkedSchema()
    }
    expectResult(Schema("a" -> Domain.Integer, "c" -> Domain.Integer)) {
      Query.Quotient(
        Query.Project(Query.Empty,
          "a" -> Expr.Null(Domain.Integer),
          "b" -> Expr.Null(Domain.Integer),
          "c" -> Expr.Null(Domain.Integer)
        ),
        Query.Project(Query.Empty, "b" -> Expr.Null(Domain.Integer))
      )
      .checkedSchema()
    }
    intercept[DomainCheckException] {
      Query.Quotient(
        Query.Project(Query.Empty,
          "a" -> Expr.Null(Domain.Integer),
          "b" -> Expr.Null(Domain.Integer)
        ),
        Query.Project(Query.Empty,
          "a" -> Expr.Null(Domain.Integer),
          "c" -> Expr.Null(Domain.Integer)
        )
      )
      .checkedSchema()
    }
  }

  test("scalar sub-query") {
    val SUBA = Query.Base("SUBA", Schema("C" -> Domain.String))
    val SUBB = Query.Base("SUBB", Schema("C" -> Domain.String))
    expectResult(Schema("C" -> Domain.String)) {
      Query.Project(Query.Restrict(SUBA,
        Expr.Application(
          Operator.Eq, Expr.ScalarSubQuery(Query.Project(SUBB, "C" -> Expr.AttributeRef("C"))),
          Expr.AttributeRef("C"))),
        "C" -> Expr.AttributeRef("C"))
        .checkedSchema()
    }
  }

  test("set sub-query") {
    expectResult(Schema("S" -> Domain.Set(Domain.Integer))) {
      Query.Project(Query.Empty, "S" -> Expr.SetSubQuery(
        Query.Project(tbl1, "two" -> Expr.AttributeRef("two"))))
        .checkedSchema()
    }
  }
}
