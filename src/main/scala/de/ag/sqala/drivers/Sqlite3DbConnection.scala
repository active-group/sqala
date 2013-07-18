package de.ag.sqala.drivers

import de.ag.sqala._
import java.io.{File, Writer}

/**
 *
 */
class Sqlite3DbConnection(connection:java.sql.Connection) extends DbConnection {
  val kind: Symbol = 'sqlite3
  val name: String = "Sqlite3"
  val handle: Handle = JDBCHandle(connection)
  val sqlWriteParameterization: SqlWriteParameterization = new SqlWriteParameterization {
    /**
     * write constant SQL expression (literal) to output sink
     *
     * @param out: output sink
     * @param value: constant literal to write
     */
    def writeLiteral(out: Writer, value: SqlLiteral) { /* FIXME */}

    /**
     * write SqlQueryCombine to output sink
     *
     * @param out output sink
     * @param param this object for recursive calls
     * @param sqlCombine left sql query, combine operator, right sql query
     */
    def writeCombine(out: Writer, param: SqlWriteParameterization, sqlCombine: SqlQueryCombine) {
      /* FIXME */
    }
  }

  def close() {
    connection.close()
  }

  def query(sqlQuery: SqlQuery, schema: Schema): Cursor = {
    val sql = sqlQuery.toString(sqlWriteParameterization)
    val statement = connection.createStatement()
    statement.executeQuery(sql)
    new Cursor() // FIXME
  }

  def insert(table: SqlTableName, schema: Schema, values: Seq[Any]): Int = ??? /*FIXME*/

  /*FIXME*/
  def delete(s: String, expr: SqlExpr): Any = ???

  /*FIXME*/
  def update(s: String, schema: Schema, expr: SqlExpr, a: Seq[(String, SqlExpr)]): Int = ???

  // FIXME structured?
  def execute(sql: String): Any = ???
}

object Sqlite3DbConnection {
  def open(file:File): Sqlite3DbConnection =
    open(file.getPath)

  def open(file:File, user:String, password:String): Sqlite3DbConnection =
    open(file.getPath, user, password)

  private def open(where:String): Sqlite3DbConnection = {
    Class.forName("org.sqlite.JDBC")
    val connection = java.sql.DriverManager.getConnection("jdbc:sqlite:" + where)
    new Sqlite3DbConnection(connection)
  }

  private def open(where:String, user:String, password:String): Sqlite3DbConnection = {
    Class.forName("org.sqlite.JDBC")
    val connection = java.sql.DriverManager.getConnection("jdbc:sqlite:" + where, user, password)
    new Sqlite3DbConnection(connection)
  }

  def openInMemory(): Sqlite3DbConnection =
    open(":memory:")
}
