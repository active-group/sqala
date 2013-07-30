package de.ag.sqala.test

import org.scalatest.FunSuite
import de.ag.sqala.relational.Schema
import de.ag.sqala.sql.Table
import de.ag.sqala.relational.{Query => RQuery, Expr => RExpr}
import de.ag.sqala.{Operator, Domain}

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
}
