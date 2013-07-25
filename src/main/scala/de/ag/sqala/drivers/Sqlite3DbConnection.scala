package de.ag.sqala.drivers

import de.ag.sqala.sql._
import java.io.{File, Writer}
import java.util.Properties
import de.ag.sqala._
import de.ag.sqala.JDBCHandle
import de.ag.sqala.sql.LiteralBoolean
import de.ag.sqala.sql.QueryCombine
import de.ag.sqala.relational.Schema

/**
 * Driver for sqlite3 database
 */
class Sqlite3DbConnection(connection:java.sql.Connection) extends DbConnection {
  val kind: Symbol = 'sqlite3
  val name: String = "sqlite3"
  val handle: Handle = JDBCHandle(connection)
  val sqlWriteParameterization: WriteParameterization = new WriteParameterization {
    /**
     * write constant SQL expression (literal) to output sink
     *
     * @param out: output sink
     * @param value: constant literal to write
     */
    def writeLiteral(out: Writer, value: Literal) {
      value match {
        case LiteralBoolean(b) => out.write(if (b) "1" else "0")
        case _ => defaultSqlWriteParameterization.writeLiteral(out, value)
      }
    }

    /**
     * write QueryCombine to output sink
     *
     * @param out output sink
     * @param param this object for recursive calls
     * @param sqlCombine left sql query, combine operator, right sql query
     */
    def writeCombine(out: Writer, param: WriteParameterization, sqlCombine: QueryCombine) {
      out.write("SELECT * FROM (")
      sqlCombine.left.write(out, param)
      sqlCombine.op match {
        case CombineOpIntersect => " INTERSECT "
        case CombineOpExcept => " EXCEPT "
        case CombineOpUnion => " UNION "
      }
      sqlCombine.right.write(out, param)
      out.write(")")
    }
  }

  def close() {
    connection.close()
  }

  def query(sqlQuery: Query, schema: Schema): ResultSetIterator = {
    val sql = sqlQuery.toString(sqlWriteParameterization)
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)
    // first shot: primitively return Objects
    new ResultSetIterator(resultSet)
  }


  def insert(tableName: Query.TableName, schema: Schema, values: Seq[AnyRef]): Int = {
    val sql = "INSERT INTO %s(%s) VALUES (%s)".format(
      tableName,
      schema.attributes.mkString(", "),
      listToPlaceholders(values).mkString(", ")
    )
    val statement = connection.prepareStatement(sql)
    values
      .zip(schema.domains)
      .zip(1 to schema.degree) // sqlite counts from 1
      .foreach {
      case ((value, domain), i) =>
        domain match {
          case Domain.String => statement.setString(i, value.asInstanceOf[String])
          case Domain.Integer => statement.setInt(i, value.asInstanceOf[Integer].intValue())
          case Domain.Double => statement.setDouble(i, value.asInstanceOf[Double])
          case Domain.Boolean => statement.setBoolean(i, value.asInstanceOf[Boolean])
          case Domain.CalendarTime => throw new IllegalArgumentException("sqlite cannot handle date/time")
          case Domain.Blob => statement.setBlob(i, value.asInstanceOf[java.io.InputStream])
          case _ => throw new RuntimeException("unknown domain " + domain)
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

  def delete(tableName: Query.TableName, condition: Expr): Int = {
    val sql = "DELETE FROM %s WHERE %s".format(tableName, condition.toString(sqlWriteParameterization))
    val statement = connection.createStatement()
    val result = statement.executeUpdate(sql)
    statement.close()
    result
  }


  def update(tableName: Query.TableName, condition: Expr, updates: Seq[(Query.ColumnName, Expr)]): Int = {
    val clauses = updates.map {
      case (columnName, value) => "%s = %s".format(columnName, value.toString(sqlWriteParameterization))
    }
    val sql = "UPDATE %s SET %s WHERE %s".format(
      tableName,
      clauses.mkString(", "),
      condition.toString(sqlWriteParameterization)
    )
    val statement = connection.createStatement()
    val result = statement.executeUpdate(sql)
    statement.close()
    result
  }

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

  // use SQLiteConfig.toProperties to generate sqlite properties
  def open(file:File, properties:Properties): Sqlite3DbConnection =
    open(file.getPath, properties)

  def openInMemory(): Sqlite3DbConnection =
    open(":memory:", new Properties())

  private def open(where: String, properties: Properties): Sqlite3DbConnection = {
    Class.forName("org.sqlite.JDBC")
    val connection = java.sql.DriverManager.getConnection("jdbc:sqlite:" + where, properties)
    new Sqlite3DbConnection(connection)
  }
}
