package de.ag.sqala

import de.ag.sqala.sql._
import de.ag.sqala.relational.Schema

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
  def read(table:Table, schema:Schema): ResultSetIterator
  def insert(table:Table.TableName, schema:Schema, values:Seq[AnyRef]): Int
  def delete(table:Table.TableName, condition:Expr): Int
  def update(table:Table.TableName, condition:Expr, updates:Seq[(String, Expr)]): Int
  def execute(sql:String):Any // run raw , for emergencies
}
