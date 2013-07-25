package de.ag.sqala.drivers

import de.ag.sqala.sql._
import java.io.{File, Writer}
import java.sql.Date
import java.util.Properties
import de.ag.sqala._
import de.ag.sqala.JDBCHandle
import de.ag.sqala.sql.Query
import de.ag.sqala.relational.Schema
import java.net.URL

/**
 * Driver for DB2 database
 */
class Db2DbConnection(connection:java.sql.Connection) extends DbConnection {
  val kind: Symbol = 'db2
  val name: String = "DB2"
  val handle: Handle = JDBCHandle(connection)
  val sqlWriteParameterization: WriteParameterization = new WriteParameterization {
    /**
     * write constant SQL expression (literal) to output sink
     *
     * @param out: output sink
     * @param value: constant literal to write
     */
    def writeLiteral(out: Writer, value: Expr.Literal) {
      value match {
        case Expr.Literal.Boolean(b) => out.write(if (b) "1" else "0")
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
    def writeCombine(out: Writer, param: WriteParameterization, sqlCombine: Query.Combine) {
      out.write("SELECT * FROM (")
      sqlCombine.left.write(out, param)
      sqlCombine.op match {
        case Expr.CombineOp.Intersect => " INTERSECT "
        case Expr.CombineOp.Except => " EXCEPT "
        case Expr.CombineOp.Union => " UNION "
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
      .zip(1 to schema.degree) // parameter count starts with 1
      .foreach {
      case ((value, domain), i) =>
        domain match {
          case Domain.String => statement.setString(i, value.asInstanceOf[String])
          case Domain.Integer => statement.setInt(i, value.asInstanceOf[Integer].intValue())
          case Domain.Double => statement.setDouble(i, value.asInstanceOf[Double])
          case Domain.Boolean => statement.setBoolean(i, value.asInstanceOf[Boolean])
          case Domain.CalendarTime => statement.setDate(i, value.asInstanceOf[Date])
          case Domain.Blob => statement.setBlob(i, value.asInstanceOf[java.io.InputStream])
          case _ => throw new RuntimeException("unknown domain " + domain)
        }
    }
    val result = statement.executeUpdate()
    statement.close()
    result
  }


  private def listToPlaceholders(values: Seq[Any]): Seq[String] = {
    values.map{_=>"?"}
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

object Db2DbConnection {
  class Location(val host:String, val port:Int, val database:String) {
    if (!(port > 0 && port <= 65535))
      throw new IllegalArgumentException("port number must be positive and below 65535, not " + port)
  }

  def open(db2Loc:Location, user:String, password:String): Db2DbConnection = {
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    open(db2Loc, props)
  }

  def open(db2Loc:Location, properties:Properties): Db2DbConnection = {
    val where = "//%s:%s/%s".format(db2Loc.host, db2Loc.port, db2Loc.database)
    open(where, properties)
  }

  private def open(where: String, properties: Properties): Db2DbConnection = {
    Class.forName("com.ibm.db2.jcc.DB2Driver")
    val connection = java.sql.DriverManager.getConnection("jdbc:db2:" + where, properties)
    new Db2DbConnection(connection)
  }
}