package de.ag.sqala.drivers

import de.ag.sqala.sql._
import java.io.{File, Writer}
import java.util.Properties
import de.ag.sqala._
import de.ag.sqala.JDBCHandle
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
     * @param sqlCombine left sql view, combine operator, right sql view
     */
    def writeCombine(out: Writer, param: WriteParameterization, sqlCombine: View.Combine) {
      out.write("SELECT * FROM (")
      sqlCombine.left.write(out, param)
      sqlCombine.op.toSpacedString
      sqlCombine.right.write(out, param)
      out.write(")")
    }
  }

  def close() {
    connection.close()
  }

  def read(view: View, schema: Schema): ResultSetIterator = {
    val sql = view.toString(sqlWriteParameterization)
    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)
    // first shot: primitively return Objects
    new ResultSetIterator(resultSet)
  }


  def insert(tableName: View.TableName, schema: Schema, values: Seq[AnyRef]): Int = {
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
          case Domain.BoundedString(_) => statement.setString(i, value.asInstanceOf[String])
          case Domain.IdentityInteger => statement.setObject(1, null)
          case Domain.Integer => statement.setInt(i, value.asInstanceOf[Integer].intValue())
          case Domain.Double => statement.setDouble(i, value.asInstanceOf[Double])
          case Domain.Boolean => statement.setBoolean(i, value.asInstanceOf[Boolean])
          case Domain.CalendarTime => statement.setString(i, value.asInstanceOf[String]) // ISO-8601 String
          case Domain.Blob => statement.setBlob(i, value.asInstanceOf[java.io.InputStream]) // FIXME
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

  def delete(tableName: View.TableName, condition: Expr): Int = {
    val sql = "DELETE FROM %s WHERE %s".format(tableName, condition.toString(sqlWriteParameterization))
    val statement = connection.createStatement()
    val result = statement.executeUpdate(sql)
    statement.close()
    result
  }


  def update(tableName: View.TableName, condition: Expr, updates: Seq[(View.ColumnName, Expr)]): Int = {
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

  def domain2SqliteDomain(domain: Domain): String = domain match {
    case Domain.String => "TEXT"
    case Domain.BoundedString(maxSize) => "VARCHAR(%d)".format(maxSize)
    case Domain.IdentityInteger => "INTEGER PRIMARY KEY AUTOINCREMENT"
    case Domain.Integer => "INTEGER"
    case Domain.Double => "REAL"
    case Domain.Blob => "BLOB"
    case Domain.CalendarTime => "TEXT" // as ISO-8601: YYYY-MM-DD HH:MM:SS.SSS
    case _ => throw new RuntimeException("not implemented")
  }

  def schemaToDDTList(schema: Schema): Seq[String] =
    schema.schema.map{
      case(attr, domain) => "%s %s".format(attr, domain2SqliteDomain(domain))
    }

  def createTable(name: View.TableName, schema: Schema): Unit = {
    val sql = "CREATE TABLE %s (\n%s\n)".format(name, schemaToDDTList(schema).mkString(",\n"))
    val statement = connection.createStatement()
    statement.execute(sql)
    statement.close()
  }


  def execute(sql: String): Either[ResultSetIterator, Int] = {
    val statement = connection.createStatement()
    if (statement.execute(sql))
      Left(new ResultSetIterator(statement.getResultSet))
    else
      Right(statement.getUpdateCount)
  }

  def dropTable(name: View.TableName) {
    val statement = connection.createStatement()
    statement.execute("DROP TABLE %s".format(name))
    statement.close()
  }

  def dropTableIfExists(name: View.TableName) {
    val statement = connection.createStatement()
    statement.execute("DROP TABLE IF EXISTS %s".format(name))
    statement.close()
  }

  /**
   * Insert values into database table and retrieve the generated keys, if any.
   *
   * @param table   Name of table to which to insert values
   * @param schema  Schema of the table that is being inserted
   * @param values  Values to insert (single row)
   * @return        Number of rows that have been inserted and list of keys generated by that insertion.
   */
  def insertAndRetrieveGeneratedKeys(table: View.TableName, schema: Schema, values: Seq[AnyRef]): (Int, Seq[AnyRef]) = ???
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
