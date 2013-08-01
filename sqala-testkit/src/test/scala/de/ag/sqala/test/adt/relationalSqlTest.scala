package de.ag.sqala.test.adt

import org.scalatest.FunSuite
import de.ag.sqala.relational.Schema
import de.ag.sqala.sql.View
import de.ag.sqala.relational.{Query => RQuery, Expr => RExpr}
import de.ag.sqala.{Ascending, Operator, Domain}

/**
 *
 */
class relationalSqlTest extends FunSuite {
  val tbl1 = View.Table("tbl1", Schema("one" -> Domain.String, "two" -> Domain.Integer))
  val query1 = tbl1.toQuery
  val tbl2 = View.Table("tbl2", Schema("three" -> Domain.Blob, "four" -> Domain.String))
  val query2 = tbl2.toQuery

  test("trivial") {
    expectResult("SELECT two, one FROM tbl1") {
      RQuery.Project(query1, "two" -> RExpr.AttributeRef("two"),
        "one" -> RExpr.AttributeRef("one"))
        .toSqlView
        .toString
    }

    expectResult("SELECT (two = two) AS eq FROM tbl1") {
      RQuery.Project(query1, "eq" -> RExpr.Application(Operator.Eq, RExpr.AttributeRef("two"), RExpr.AttributeRef("two")))
      .toSqlView
        .toString
    }

    expectResult("SELECT * FROM tbl1, tbl2") {
      RQuery.Product(query1, query2)
      .toSqlView
      .toString
    }
  }

  test("case") {
    expectResult("SELECT (CASE WHEN (two = two) THEN one ELSE one) AS foo FROM tbl1") {
      RQuery.Project(query1, "foo" -> RExpr.Case(
        Seq(RExpr.CaseBranch(RExpr.Application(Operator.Eq, RExpr.AttributeRef("two"), RExpr.AttributeRef("two")), RExpr.AttributeRef("one"))),
        Some(RExpr.AttributeRef("one"))))
      .toSqlView
      .toString
    }
  }

  test("aggregation") {
    expectResult("SELECT one, AVG(two) AS foo FROM tbl1 GROUP BY one") {
      RQuery.GroupingProject(query1, "one" -> RExpr.AttributeRef("one"),
      "foo" -> RExpr.Aggregation(Operator.Avg, RExpr.AttributeRef("two")))
      .toSqlView
      .toString()
    }

    expectResult("SELECT two, COUNT(one) AS foo FROM tbl1 GROUP BY two") {
      RQuery.GroupingProject(query1, "two" -> RExpr.AttributeRef("two"),
        "foo" -> RExpr.Aggregation(Operator.Count, RExpr.AttributeRef("one")))
        .toSqlView
        .toString()
    }
  }

  test("order") {
    expectResult("SELECT * FROM tbl1 ORDER BY one ASC") {
      RQuery.Order(query1, RExpr.AttributeRef("one") -> Ascending)
      .toSqlView
      .toString
    }
  }

  test("subquery") {
    expectResult("SELECT * FROM tbl2 WHERE (SELECT * FROM tbl1)") {
      RQuery.Restrict(query2, RExpr.ScalarSubQuery(query1))
      .toSqlView
      .toString
    }
  }

  test("quotient-1") {
    val R = RQuery.Base("R", Schema("A" -> Domain.String, "B" -> Domain.String))
    val S = RQuery.Base("S", Schema("A" -> Domain.String))
    expectResult("SELECT B FROM R WHERE (A IN (SELECT * FROM S)) GROUP BY B HAVING (COUNT(B) = (SELECT COUNT(A) FROM S))") {
      RQuery.Quotient(R, S)
      .toSqlView
      .toString
    }
  }
}
