package de.ag.sqala.drivers

import de.ag.sqala._
import java.io.{File, Writer}
import org.sqlite.SQLiteConfig
import java.util.Properties

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
    def writeLiteral(out: Writer, value: SqlLiteral) {
      value match {
        case SqlLiteralBoolean(b) => out.write(if (b) "1" else "0")
        case _ => defaultSqlWriteParameterization.writeLiteral(out, value)
      }
    }

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

  def query(sqlQuery: SqlQuery, schema: Schema): ResultSetIterator = {
    val sql = sqlQuery.toString(sqlWriteParameterization)
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)
    // first shot: primitively return Objects
    new ResultSetIterator(resultSet)
  }


  def insert(tableName: SqlTableName, schema: Schema, values: Seq[AnyRef]): Int = {
    val sql = "INSERT INTO %s(%s) VALUES (%s)".format(
      tableName,
      schema.labels.mkString(", "),
      listToPlaceholders(values).mkString(", ")
    )
    val statement = connection.prepareStatement(sql)
    values
      .zip(schema.domains)
      .zip(1 to schema.degree) // sqlite counts from 1
      .foreach {
      case ((value, domain), i) =>
        domain match {
          case DBString => statement.setString(i, value.asInstanceOf[String])
          case DBInteger => statement.setInt(i, value.asInstanceOf[Integer].intValue())
          case DBDouble => statement.setDouble(i, value.asInstanceOf[Double])
          case DBBoolean => statement.setBoolean(i, value.asInstanceOf[Boolean])
          case DBCalendarTime => throw new IllegalArgumentException("sqlite cannot handle date/time")
          case DBBlob => statement.setBlob(i, value.asInstanceOf[java.io.InputStream])
          case _ => throw new RuntimeException("unknown type " + domain)
        }
    }
    val result = statement.executeUpdate()
    statement.close()
    result
  }


  private def listToPlaceholders(values: Seq[Any]): Seq[String] = {
    values.zipWithIndex.map({
      case (_, i) => ":" + i
    })
  }

  def delete(tableName: SqlTableName, expr: SqlExpr): Int = {
    val sql = "DELETE FROM %s WHERE %s".format(tableName, expr.toString(sqlWriteParameterization))
    val statement = connection.createStatement()
    val result = statement.executeUpdate(sql)
    statement.close()
    result
  }

  def update(s: String, schema: Schema, expr: SqlExpr, a: Seq[(String, SqlExpr)]): Int = ???

  // FIXME structured?
  def execute(sql: String): Either[ResultSetIterator, Int] = {
    val statement = connection.createStatement()
    if (statement.execute(sql))
      Left(new ResultSetIterator(statement.getResultSet))
    else
      Right(statement.getUpdateCount)
  }
}

object Sqlite3DbConnection {
  def open(file:File): Sqlite3DbConnection =
    open(file.getPath, new Properties())

  def open(file:File, user:String, password:String): Sqlite3DbConnection = {
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    open(file.getPath, props)
  }

  def open(file:File, config:SQLiteConfig): Sqlite3DbConnection =
    open(file.getPath, config.toProperties)

  def openInMemory(): Sqlite3DbConnection =
    open(":memory:", new Properties())

  private def open(where: String, properties: Properties): Sqlite3DbConnection = {
    Class.forName("org.sqlite.JDBC")
    val connection = java.sql.DriverManager.getConnection("jdbc:sqlite:" + where, properties)
    new Sqlite3DbConnection(connection)
  }
}
