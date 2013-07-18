package de.ag.sqala

class Cursor //FIXME

sealed abstract class Handle
case class JDBCHandle(connection: java.sql.Connection) extends Handle

class ResultSetIterator(resultSet:java.sql.ResultSet) extends Iterator[Seq[AnyRef]] {
  private val metaData = resultSet.getMetaData
  private val columnCount = metaData.getColumnCount
  private var needNext = true
  private var depleted = false
  def next():Seq[AnyRef] = {
    maybeAdvanceResultSetCursor()
    needNext = true
    (1 to columnCount).map(resultSet.getObject)
  }

  def hasNext = {
    maybeAdvanceResultSetCursor()
    depleted
  }

  /* ResultSet API only tells me whether there is a next by actually
     calling next().  This contrasts Iterator's hasNext, which can be
     called many times.  So we keep track whether we've called next and
     whether we're at the end of the set.
  */
  private def maybeAdvanceResultSetCursor() {
    if (needNext) {
      depleted = resultSet.next()
      needNext = false
    }
  }

}


/**
 *
 */
trait DbConnection {
  val kind: Symbol       // what kind of database we're attached to
  val name: String       // same, for humans
  val handle: Handle     // DB-specific connection handle (?)
  val sqlWriteParameterization: SqlWriteParameterization

  def close():Unit
  def query(sqlQuery:SqlQuery, schema:Schema): ResultSetIterator
  def insert(table:SqlTableName, schema:Schema, values:Seq[AnyRef/*FIXME*/]): Int /*FIXME*/ // FIXME structured?
  def delete(table:SqlTableName, expr:SqlExpr): Any /*FIXME*/ // FIXME structured?
  def update(table:SqlTableName, schema:Schema, expr:SqlExpr, a:Seq[(String, SqlExpr)]): Int // FIXME structured?
  def execute(sql:String):Any /*FIXME*/ // run raw SQL, for emergencies
}
