package de.ag.sqala.test

import org.scalatest.{BeforeAndAfter, FunSuite}
import de.ag.sqala.drivers.Sqlite3DbConnection
import de.ag.sqala._

/**
 *
 */
class SqliteTest extends FunSuite with BeforeAndAfter {
  var conn:DbConnection = _

  before {
    conn = Sqlite3DbConnection.openInMemory()
  }



  test("open/close") {}
  test("create tables") {
    createTbl1()
  }

  def createTbl1() {
    conn.execute("CREATE TABLE tbl1(one VARCHAR(10), two SMALLINT)")
  }

  val tbl1Schema: Schema = new Schema(Seq(("one", DBString), ("two", DBInteger)))

  test("insert") {
    createTbl1()
    expectResult(1){conn.insert("tbl1", tbl1Schema, Seq("test", Integer.valueOf(10)))}
  }

  test("insert & query") {
    createTbl1()
    assert(1 == conn.insert("tbl1", tbl1Schema, Seq("test", Integer.valueOf(10))))

    val results = conn.query(SqlQuery.makeSelect(
      attributes = Seq(SqlQuerySelectAttribute(SqlExprColumn("one"), None), SqlQuerySelectAttribute(SqlExprColumn("two"), None)),
      from = Seq(SqlQuerySelectFrom(SqlQueryTable("tbl1"), None))
    ),
      new Schema(Seq(("one", DBString), ("two", DBInteger))))
      .toArray

    expectResult(1) {
      results.size
    }
    expectResult(Seq("test", 10)) {
      results(0)
    }
  }

  test("insert & query many") {
    createTbl1()
    val data = Seq(
      ("test", 10),
      ("foo", 12),
      ("bar", -1),
      ("filderstadt", 70794)
    )
    data.foreach { case (s,i) =>
      conn.insert("tbl1", tbl1Schema, Seq(s, Integer.valueOf(i)))}

    expectResult(data.map{d => Seq(d._1, d._2)}.toSet){
      conn.query(SqlQueryTable("tbl1"), tbl1Schema)
        .toSet
    }
  }

  test("delete") {
    createTbl1()
    val data = Seq(
      ("test", 10),
      ("foo", 12),
      ("bar", -1),
      ("filderstadt", 70794)
    )
    data.foreach { case (s,i) =>
      conn.insert("tbl1", tbl1Schema, Seq(s, Integer.valueOf(i)))}

    expectResult(1){conn.delete("tbl1", SqlExprApp(SqlOperatorEq, Seq(SqlExprColumn("one"), SqlExprConst(SqlLiteralString("test")))))}
    expectResult(2){conn.delete("tbl1", SqlExprApp(SqlOperatorOr, Seq(
        SqlExprApp(SqlOperatorEq, Seq(SqlExprColumn("one"), SqlExprConst(SqlLiteralString("foo")))),
        SqlExprApp(SqlOperatorEq, Seq(SqlExprColumn("two"), SqlExprConst(SqlLiteralNumber(-1)))))))}
    expectResult(0){conn.delete("tbl1", SqlExprApp(SqlOperatorEq, Seq(SqlExprColumn("one"), SqlExprConst(SqlLiteralString("test")))))}
  }
}
