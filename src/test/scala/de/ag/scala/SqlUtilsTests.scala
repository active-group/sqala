package de.ag.scala

import de.ag.sqala._
import minitest._

object SqlUtilsTests extends SimpleTestSuite {

  val att1 = Seq(("first", SqlExpressionColumn("first")), ("abc", SqlExpressionColumn("second")))
  val att2 = Seq(("blub", SqlExpressionConst(Type.string, "")), ("blubStr", SqlExpressionConst(Type.string, "X")),
    ("blub2", SqlExpressionConst(Type.integer, 4)))

  test("attributes (& putLiteral)") {
    assertEquals(SQL.attributes(Seq.empty), Some(("*", Seq.empty)))
    assertEquals(SQL.attributes(att1), Some(("first, second AS abc", Seq.empty)))
    assertEquals(SQL.attributes(att2), Some(("? AS blub, ? AS blubStr, ? AS blub2",
      Seq((Type.string, ""), (Type.string, "X"), (Type.integer, 4)))))
    // TODO Test more
  }

  val tbl1 = SqlSelectTable("tabelleA", RelationalScheme.make(Seq(("a", Type.string), ("b", Type.string))))
  val tbl2 = SqlSelectTable("tabelleB", RelationalScheme.make(Seq(("b", Type.string), ("c", Type.string))))
  val tbl3 = SQL.makeSqlSelect(Seq(("a", SqlExpressionColumn("b"))), Seq((Some("t1"), tbl1)))

  test("join") {
    assertEquals(SQL.join(Seq((None, tbl1)), Seq.empty, Seq.empty), Some(("FROM tabelleA", Seq.empty)))
    assertEquals(SQL.join(Seq((Some("A"), tbl1)), Seq.empty, Seq.empty), Some(("FROM tabelleA AS A", Seq.empty)))
    assertEquals(SQL.join(Seq((None, tbl1), (None, tbl2), (Some("tdx"), tbl3)), Seq.empty, Seq.empty),
      Some(("FROM tabelleA, tabelleB, (SELECT b AS a FROM tabelleA AS t1) AS tdx", Seq.empty)))
    assertEquals(SQL.join(Seq((None, tbl1)), Seq((Some("tx"), tbl2)),
      Seq(SqlExpressionApp(SqlOperator.eq, Seq(SqlExpressionColumn("a"), SqlExpressionColumn("b"))))),
      Some(("FROM tabelleA LEFT JOIN tabelleB AS tx ON (a = b)", Seq.empty)))
    assertEquals(SQL.join(Seq((None, tbl1), (Some("tdx"), tbl3)), Seq((None, tbl2)),
      Seq(SqlExpressionApp(SqlOperator.eq, Seq(SqlExpressionColumn("a"), SqlExpressionColumn("b"))))),
      Some(("FROM (SELECT * FROM tabelleA, (SELECT b AS a FROM tabelleA AS t1) AS tdx) LEFT JOIN tabelleB ON (a = b)", Seq.empty)))
    assertEquals(SQL.join(Seq((None, tbl1)), Seq((Some("t1"), tbl1), (Some("t2"), tbl2), (Some("tt"), tbl1)),
      Seq(SqlExpressionApp(SqlOperator.eq, Seq(SqlExpressionColumn("b"), SqlExpressionColumn("a"))))),
      Some(("FROM tabelleA LEFT JOIN tabelleA AS t1 ON (1=1) LEFT JOIN tabelleB AS t2 ON (1=1) LEFT JOIN tabelleA AS tt ON (b = a)", Seq.empty)))
  }

  test("surround and concat SQL") {
    assertEquals(SqlUtils.surroundSQL("", ("blublba", Seq((Type.string, "bla"))), ""), ("blublba", Seq((Type.string, "bla"))))
    assertEquals(SqlUtils.surroundSQL("y", ("blublba", Seq((Type.string, "bla"))), "<"), ("yblublba<", Seq((Type.string, "bla"))))
    assertEquals(SqlUtils.concatSQL(Seq.empty), ("", Seq.empty))
    assertEquals(SqlUtils.concatSQL(Seq(
      ("a", Seq((Type.boolean, true))),
      ("b", Seq((Type.integer, 3), (Type.string, "a"))),
      ("d", Seq.empty),
      ("c", Seq((Type.integer, -1))))),
      ("abdc", Seq((Type.boolean, true), (Type.integer, 3), (Type.string, "a"), (Type.integer, -1))))
  }

  /*

  Ein paar Gedankengänge ...

SELECT *
FROM (VALUES (1, 'b'), (2, 'a')) AS t1 (id,nam)
LEFT OUTER JOIN (VALUES (3, 'c'), (4,'b')) AS t2 (id,nam) ON (1=1)
LEFT OUTER JOIN (VALUES (5,'x'), (6,'x')) t3 (id,nam) ON t1.nam = t2.nam AND t1.nam = t3.nam

=>
  Cols t1 |  Cols t2 |  Cols t3
 id | nam | id | nam | id | nam
----+-----+----+-----+----+-----
 1  |  b  | 3  |  c  |    |
 1  |  b  | 4  |  b  |    |
 2  |  a  | 3  |  c  |    |
 2  |  a  | 4  |  b  |    |




SELECT *
FROM (VALUES (1, 'b'), (2, 'a')) AS t1 (id,nam)
LEFT OUTER JOIN (VALUES (3, 'c'), (4,'b')) AS t2 (id,nam) ON t1.nam = t2.nam
LEFT OUTER JOIN (VALUES (5,'x'), (6,'x')) t3 (id,nam) ON t1.nam = t3.nam

=>
  Cols t1 |  Cols t2 |  Cols t3
 id | nam | id | nam | id | nam
----+-----+----+-----+----+-----
 1  |  b  | 4  |  b  |    |
 2  |  a  |    |     |    |


 => (1=1) kann nicht pauschal bei LEFT JOIN verwendet werden.


In Sqloure:

  (sqlosure.sql-put/sql-select->string
          (query->sql
           (opt/optimize-query
            (get-query
             (monadic
              [t1 (embed (make-sql-table "t1"
                                         (alist->rel-scheme [["id" integer%]
                                                             ["nam" string%]])
                                         :universe test-universe))]
              [t2
               (outer (make-sql-table "t2"
                                         (alist->rel-scheme [["id" integer%]
                                                             ["nam" string%]])
                                         :universe test-universe))]
              (restrict-outer (=$ (! t1 "nam")
                                  (! t2 "nam"))) ;; muss hier gesetzt sein, sonst falsches Ergebnis!
              [t3
               (outer (make-sql-table "t3"
                                      (alist->rel-scheme [["id" integer%]
                                                          ["nam" string%]])
                                      :universe test-universe))]
              (restrict-outer (=$ (! t1 "nam")
                                  (! t3 "nam")))
              (project [["fo1" (! t1 "nam")]
                        ["fo2" (! t2 "nam")]
                        ["fo3" (! t3 "nam")]]))))))

   */
}
