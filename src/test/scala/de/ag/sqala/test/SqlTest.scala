package de.ag.sqala.test

import org.scalatest.FunSuite
import de.ag.sqala.{SqlQuerySelectFrom, SqlQuery, SqlQuerySelect, SqlQueryTable}

/**
 *
 */
class SqlTest extends FunSuite {
  test("SqlQueryTable") {
    val sqlQuery = SqlQueryTable("tbl1")
    expectResult("SELECT * FROM tbl1"){sqlQuery.toString}
  }
  test("simple SqlQuerySelect") {
    val sqlQuery = SqlQuery.makeSelect(from = Seq(SqlQuerySelectFrom(SqlQueryTable("tbl1"), None)))
    expectResult("SELECT * FROM tbl1"){sqlQuery.toString}
  }
}
