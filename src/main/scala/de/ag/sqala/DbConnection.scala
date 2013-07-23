package de.ag.sqala

import de.ag.sqala.sql._
import de.ag.sqala.relational.Schema

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
  val sqlWriteParameterization: WriteParameterization

  def close():Unit
  def query(Query:Query, schema:Schema): ResultSetIterator
  def insert(table:TableName, schema:Schema, values:Seq[AnyRef/*FIXME*/]): Int /*FIXME*/ // FIXME structured?
  def delete(table:TableName, expr:Expr): Any /*FIXME*/ // FIXME structured?
  def update(table:TableName, expr:Expr, a:Seq[(String, Expr)]): Int // FIXME structured?
  def execute(sql:String):Any /*FIXME*/ // run raw , for emergencies
}
