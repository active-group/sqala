package de.ag.sqala.test

import org.scalatest.{BeforeAndAfter, FunSuite}
import de.ag.sqala.drivers.Db2DbConnection
import de.ag.sqala.sql._
import de.ag.sqala.{ResultSetIterator, Domain, DbConnection, Operator}
import de.ag.sqala.relational.Schema
import de.ag.sqala.relational.Query.Base

/**
 *
 */
class Db2Test extends FunSuite with BeforeAndAfter {
  var conn:DbConnection = _
  val where = new Db2DbConnection.Location("192.168.1.138", 50001, "test")

  before {
    conn = Db2DbConnection.open(where, "db2inst2", "db2inst2")
  }

  val tbl1Schema: Schema = new Schema(Seq(("one", Domain.String), ("two", Domain.Integer)))

  val data:Seq[(String, java.lang.Integer)] = Seq(
    ("test", 10),
    ("foo", 12),
    ("bar", -1),
    ("filderstadt", 70794)
  )

  def createTbl1() {
    // DROP TABLE IF EXISTS
    conn.execute("select tabname from syscat.tables where tabschema='DB2INST2' and tabname='TBL1'") match {
      case Left(it:ResultSetIterator) =>
        if (it.size > 0)
          conn.execute("DROP TABLE tbl1")
      case Right(count) => throw new RuntimeException("unexpectedly received update count")
    }
    conn.execute("CREATE TABLE tbl1(one VARCHAR(11), two INT)")
  }

  def createAndFillTbl1() {
    createTbl1()
    data.foreach {
      case (w, s) =>
        conn.insert("tbl1", tbl1Schema, Seq(w, Int.box(s)))
    }
  }

  test("open/close") {}

  test("create tables") {
    createTbl1()
  }

  test("insert") {
    createTbl1()
    expectResult(1){conn.insert("tbl1", tbl1Schema, Seq("test", Int.box(10)))}
  }

  test("insert & query") {
    createTbl1()
    assert(1 == conn.insert("tbl1", tbl1Schema, Seq("test", Int.box(10))))

    val results = conn.query(Query.makeSelect(
      attributes = Seq(Query.SelectAttribute(Expr.Column("one"), None), Query.SelectAttribute(Expr.Column("two"), None)),
      from = Seq(Query.SelectFromQuery(Query.Base(Base("tbl1", tbl1Schema)), None))
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
      conn.query(Query.Base(Base("tbl1", tbl1Schema)), tbl1Schema)
        .toSet
    }
  }

  test("delete") {
    createAndFillTbl1()

    expectResult(1){conn.delete("tbl1", Expr.App(Operator.Eq, Seq(Expr.Column("one"), Expr.Const(Expr.Literal.String("test")))))}
    expectResult(2){conn.delete("tbl1", Expr.App(Operator.Or, Seq(
      Expr.App(Operator.Eq, Seq(Expr.Column("one"), Expr.Const(Expr.Literal.String("foo")))),
      Expr.App(Operator.Eq, Seq(Expr.Column("two"), Expr.Const(Expr.Literal.Integer(-1)))))))}
    expectResult(0){conn.delete("tbl1", Expr.App(Operator.Eq, Seq(Expr.Column("one"), Expr.Const(Expr.Literal.String("test")))))}
  }

  test("update") {
    createAndFillTbl1()

    expectResult(1){conn.update("tbl1",
      Expr.App(Operator.Eq, Seq(Expr.Column("one"), Expr.Const(Expr.Literal.String("bar")))),
      Seq(("two", Expr.Const(Expr.Literal.Integer(12)))))}
    expectResult(0){conn.update("tbl1",
      Expr.App(Operator.Eq, Seq(Expr.Column("one"), Expr.Const(Expr.Literal.String("not there")))),
      Seq(("two", Expr.Const(Expr.Literal.Integer(12)))))}
    expectResult(2){conn.update("tbl1",
      Expr.App(Operator.Eq, Seq(Expr.Column("two"), Expr.Const(Expr.Literal.Integer(12)))),
      Seq(("two", Expr.Const(Expr.Literal.Null))))}
  }
}
