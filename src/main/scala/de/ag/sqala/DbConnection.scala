package de.ag.sqala

import de.ag.sqala.sql._
import de.ag.sqala.relational.Schema

/**
 * Driver specific object that holds actual connection to database.
 *
 * Currently only JDBC is supported.
 */
sealed abstract class Handle
case class JDBCHandle(connection: java.sql.Connection) extends Handle

/**
 * Iterator of the result of a database read operation.
 *
 * Basically wrap java.sql.ResultSet into an Scala iterator over rows.
 * @param resultSet Underlying java.sql.ResultSet
 */
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
 * (Open) Connection to a database via a specific driver
 */
trait DbConnection {
  /** what kind of database we're attached to (choose freely but uniquely...) */
  val kind: Symbol
  /** what kind of database we're attached to (for humans; choose freely but uniquely...) */
  val name: String
  /** DB-specific connection handle */
  val handle: Handle
  /** parameterization of writing SQL strings */
  val sqlWriteParameterization: WriteParameterization

  /** Close this connection. All subsequent methods calls are undefined. */
  def close():Unit
  /** Read results from database described in table, expecting schema */
  def read(table:Table, schema:Schema): ResultSetIterator

  /** Insert values into database
    *
    * @param table   Name of table to which to add values
    * @param schema  Schema of the table that is being appended
    * @param values  Values to add (single row)
    * @return        Number of rows that have been added
    */
  def insert(table:Table.TableName, schema:Schema, values:Seq[AnyRef]): Int

  /** Delete values from database
    *
    * @param table      Name of table from which to delete values
    * @param condition  Condition that each row that is to be deleted fulfills (use tautology to delete all rows)
    * @return           Number of rows that have been deleted
    */
  def delete(table:Table.TableName, condition:Expr): Int

  /** Update values in database
    *
    * @param table      Name of table which is to be updated
    * @param condition  Condition that each row that is to be updated fulfills (use tautology to update all rows)
    * @param updates    Which column is to be updated to which value (expression)
    * @return           Number of rows that have been updated
    */
  def update(table:Table.TableName, condition:Expr, updates:Seq[(Schema.Attribute, Expr)]): Int

  /** Execute raw SQL string. For 'emergencies' and database- and driver-specific stuff.
    *
    * @param sql Raw SQL string that is sent to driver
    * @return    Whatever driver returns for that SQL. For example, JDBC returns ResultSet for 'queries'
    *            or Integer for 'updates'
    */
  def execute(sql:String):Any // run raw , for emergencies
}
