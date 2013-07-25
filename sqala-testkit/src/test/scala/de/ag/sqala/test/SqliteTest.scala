package de.ag.sqala.test

import org.scalatest.{BeforeAndAfter, FunSuite}
import de.ag.sqala.drivers.Sqlite3DbConnection
import de.ag.sqala.sql._
import de.ag.sqala.{Domain, DbConnection}
import de.ag.sqala.relational.Schema
import de.ag.sqala.Operator

/**
 *
 */
class SqliteTest extends FunSuite with BeforeAndAfter {
  var conn:DbConnection = _

  before {
    conn = Sqlite3DbConnection.openInMemory()
  }

  val tbl1Schema: Schema = new Schema(Seq(("one", Domain.String), ("two", Domain.Integer)))

  val data = Seq(
    ("test", 10),
    ("foo", 12),
    ("bar", -1),
    ("filderstadt", 70794)
  )

  def createTbl1() {
    conn.execute("CREATE TABLE tbl1(one VARCHAR(10), two SMALLINT)")
  }

  def createAndFillTbl1() {
    createTbl1()
    data.foreach {
      case (s, i) =>
        conn.insert("tbl1", tbl1Schema, Seq(s, Integer.valueOf(i)))
    }
  }

  test("open/close") {}

  test("create tables") {
    createTbl1()
  }

  test("insert") {
    createTbl1()
    expectResult(1){conn.insert("tbl1", tbl1Schema, Seq("test", Integer.valueOf(10)))}
  }

  test("insert & query") {
    createTbl1()
    assert(1 == conn.insert("tbl1", tbl1Schema, Seq("test", Integer.valueOf(10))))

    val results = conn.query(Query.makeSelect(
      attributes = Seq(Query.SelectAttribute(ExprColumn("one"), None), Query.SelectAttribute(ExprColumn("two"), None)),
      from = Seq(Query.SelectFromQuery(Query.Table("tbl1"), None))
    ),
      new Schema(Seq(("one", Domain.String), ("two", Domain.Integer))))
      .toArray

    expectResult(1) {
      results.size
    }
    expectResult(Seq("test", 10)) {
      results(0)
    }
  }

  test("insert & query many") {
    createAndFillTbl1()
    expectResult(data.map{d => Seq(d._1, d._2)}.toSet){
      conn.query(Query.Table("tbl1"), tbl1Schema)
        .toSet
    }
  }

  test("delete") {
    createAndFillTbl1()

    expectResult(1){conn.delete("tbl1", ExprApp(Operator.Eq, Seq(ExprColumn("one"), ExprConst(LiteralString("test")))))}
    expectResult(2){conn.delete("tbl1", ExprApp(Operator.Or, Seq(
        ExprApp(Operator.Eq, Seq(ExprColumn("one"), ExprConst(LiteralString("foo")))),
        ExprApp(Operator.Eq, Seq(ExprColumn("two"), ExprConst(LiteralInteger(-1)))))))}
    expectResult(0){conn.delete("tbl1", ExprApp(Operator.Eq, Seq(ExprColumn("one"), ExprConst(LiteralString("test")))))}
  }

  test("update") {
    createAndFillTbl1()

    expectResult(1){conn.update("tbl1",
      ExprApp(Operator.Eq, Seq(ExprColumn("one"), ExprConst(LiteralString("bar")))),
      Seq(("two", ExprConst(LiteralInteger(12)))))}
    expectResult(0){conn.update("tbl1",
      ExprApp(Operator.Eq, Seq(ExprColumn("one"), ExprConst(LiteralString("not there")))),
      Seq(("two", ExprConst(LiteralInteger(12)))))}
    expectResult(2){conn.update("tbl1",
      ExprApp(Operator.Eq, Seq(ExprColumn("two"), ExprConst(LiteralInteger(12)))),
      Seq(("two", ExprConst(LiteralNull))))}
  }
}
