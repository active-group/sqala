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
      "two" -> ExprAttributeRef("two"),
      "one" -> ExprAttributeRef("one")),
      tbl1)
    expectResult(Schema(Seq("two" -> Domain.Integer, "one" -> Domain.String))) {
      q.checkedSchema()
    }
  }

  test("trivial-checkDomain") {
    val q1 = QueryProject(Seq(
      "eq" -> ExprApplication(Operator.Eq, Seq(ExprAttributeRef("two"), ExprAttributeRef("two")))
    ),
      tbl1)
    q1.checkDomains() // should not throw DomainCheckException (or any other, that is)
    info("checked domains of q1 schema")
    val q2 = QueryProject(Seq(
      "eq" -> ExprApplication(Operator.Eq, Seq(ExprAttributeRef("one"), ExprAttributeRef("two")))
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
      ExprCase(Seq(
        CaseBranch(ExprApplication(Operator.Eq, Seq(ExprAttributeRef("two"), ExprAttributeRef("two"))),
          ExprAttributeRef("one"))),
        None)),
      tbl1)
    val q2 = QueryProject(Seq("foo" ->
      ExprCase(Seq(CaseBranch(ExprApplication(Operator.Eq, Seq(ExprAttributeRef("two"), ExprAttributeRef("two"))),
        ExprAttributeRef("one"))),
        Some(ExprAttributeRef("one")))),
      tbl1)
    val q3 = QueryProject(Seq("foo" ->
      ExprCase(Seq(CaseBranch(ExprApplication(Operator.Eq, Seq(ExprAttributeRef("two"), ExprAttributeRef("two"))),
        ExprAttributeRef("one"))),
        Some(ExprAttributeRef("two")))),
      tbl1)
    val q1Schema = Schema(Seq("foo" -> Domain.String))
    expectResult(q1Schema) { q1.checkedSchema()}
    expectResult(q1Schema) { q2.checkedSchema()}
    intercept[DomainCheckException] { q3.checkDomains()}
  }

  test("ordered") {
    val q1 = QueryOrder(Seq(ExprAttributeRef("one") -> Ascending), tbl1)
    expectResult(tbl1.base) { q1.checkedSchema() }
    intercept[NoSuchElementException] {
      QueryOrder(Seq(ExprAttributeRef("three") -> Ascending), tbl1).checkDomains()
    }
  }

  test("aggregation") {
    val q1 = QueryProject(Seq("foo" -> ExprAttributeRef("foo")),
      QueryGroupingProject(Seq("one" -> ExprAttributeRef("one"), "foo" -> ExprAggregation(Operator.Avg, ExprAttributeRef("two"))), tbl1))
    val q2 = QueryProject(Seq("foo" -> ExprAttributeRef("foo")),
      QueryGroupingProject(Seq("two" -> ExprAttributeRef("two"), "foo" -> ExprAggregation(Operator.Count, ExprAttributeRef("one"))), tbl1))
    expectResult(Schema(Seq("foo" -> Domain.Integer))) { q1.checkedSchema() }
    intercept[DomainCheckException]{
      QueryGroupingProject(Seq("two" -> ExprAttributeRef("two"), "foo" -> ExprAggregation(Operator.Avg, ExprAttributeRef("one"))), tbl1).checkedSchema()
    }
    expectResult(Schema(Seq("foo" -> Domain.Integer))) { q2.checkedSchema() }
  }

  test("quotient") {
    expectResult(Schema(Seq("a" -> Domain.Integer))) {
      QueryQuotient(
        QueryProject(Seq(
          "a" -> ExprNull(Domain.Integer),
          "b" -> ExprNull(Domain.Integer)
        ),
          QueryEmpty),
        QueryProject(Seq("b" -> ExprNull(Domain.Integer)), QueryEmpty)
      )
        .checkedSchema()
    }
    expectResult(Schema(Seq("a" -> Domain.Integer, "c" -> Domain.Integer))) {
      QueryQuotient(
        QueryProject(Seq(
          "a" -> ExprNull(Domain.Integer),
          "b" -> ExprNull(Domain.Integer),
          "c" -> ExprNull(Domain.Integer)
        ),
          QueryEmpty
        ),
        QueryProject(Seq("b" -> ExprNull(Domain.Integer)), QueryEmpty)
      )
      .checkedSchema()
    }
    intercept[DomainCheckException] {
      QueryQuotient(
        QueryProject(Seq(
          "a" -> ExprNull(Domain.Integer),
          "b" -> ExprNull(Domain.Integer)
        ),
          QueryEmpty),
        QueryProject(Seq(
          "a" -> ExprNull(Domain.Integer),
          "c" -> ExprNull(Domain.Integer)
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
      QueryProject(Seq("C" -> ExprAttributeRef("C")),
        QueryRestrict(
          ExprApplication(
            Operator.Eq, Seq(ExprScalarSubQuery(QueryProject(Seq("C" -> ExprAttributeRef("C")),
              SUBB)),
              ExprAttributeRef("C"))),
          SUBA))
        .checkedSchema()
    }
  }

  test("set sub-query") {
    expectResult(Schema(Seq("S" -> Domain.Set(Domain.Integer)))) {
      QueryProject(Seq("S" -> ExprSetSubQuery(
        QueryProject(Seq("two" -> ExprAttributeRef("two")), tbl1))), QueryEmpty)
        .checkedSchema()
    }
  }
}
