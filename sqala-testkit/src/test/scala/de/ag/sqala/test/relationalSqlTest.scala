package de.ag.sqala.test

import org.scalatest.FunSuite
import de.ag.sqala.relational.Schema
import de.ag.sqala.sql.Table
import de.ag.sqala.relational.{Query => RQuery, Expr => RExpr}
import de.ag.sqala.{Ascending, Operator, Domain}

/**
 *
 */
class relationalSqlTest extends FunSuite {
  val query1 = RQuery.Base("tbl1", Schema(Seq("one" -> Domain.String, "two" -> Domain.Integer)))
  val tbl1 = Table.Base(query1)
  val query2 = RQuery.Base("tbl2", Schema(Seq("three" -> Domain.Blob, "four" -> Domain.String)))
  val tbl2 = Table.Base(query2)

  test("trivial") {
    expectResult("SELECT two, one FROM tbl1") {
      RQuery.Project(Seq("two" -> RExpr.AttributeRef("two"),
        "one" -> RExpr.AttributeRef("one")),
        query1)
        .toSqlTable
        .toString
    }

    expectResult("SELECT (two = two) AS eq FROM tbl1") {
      RQuery.Project(Seq("eq" -> RExpr.Application(Operator.Eq, Seq(RExpr.AttributeRef("two"), RExpr.AttributeRef("two")))),
        query1)
        .toSqlTable
        .toString
    }

    expectResult("SELECT * FROM tbl1, tbl2") {
      RQuery.Product(query1, query2)
      .toSqlTable
      .toString
    }
  }

  test("case") {
    expectResult("SELECT (CASE WHEN (two = two) THEN one ELSE one) AS foo FROM tbl1") {
      RQuery.Project(Seq("foo" -> RExpr.Case(
        Seq(RExpr.CaseBranch(RExpr.Application(Operator.Eq, Seq(RExpr.AttributeRef("two"), RExpr.AttributeRef("two"))), RExpr.AttributeRef("one"))),
        Some(RExpr.AttributeRef("one")))),
      query1)
      .toSqlTable
      .toString
    }
  }

  test("aggregation") {
    expectResult("SELECT one, AVG(two) AS foo FROM tbl1 GROUP BY one") {
      RQuery.GroupingProject(Seq("one" -> RExpr.AttributeRef("one"),
      "foo" -> RExpr.Aggregation(Operator.Avg, RExpr.AttributeRef("two"))), query1)
      .toSqlTable
      .toString()
    }

    expectResult("SELECT two, COUNT(one) AS foo FROM tbl1 GROUP BY two") {
      RQuery.GroupingProject(Seq("two" -> RExpr.AttributeRef("two"),
        "foo" -> RExpr.Aggregation(Operator.Count, RExpr.AttributeRef("one"))), query1)
        .toSqlTable
        .toString()
    }
  }

  test("order") {
    expectResult("SELECT * FROM tbl1 ORDER BY one ASC") {
      RQuery.Order(Seq(RExpr.AttributeRef("one") -> Ascending), query1)
      .toSqlTable
      .toString
    }
  }

  test("subquery") {
    expectResult("SELECT * FROM tbl2 WHERE (SELECT * FROM tbl1)") {
      RQuery.Restrict(RExpr.ScalarSubQuery(query1), query2)
      .toSqlTable
      .toString
    }
  }


}
