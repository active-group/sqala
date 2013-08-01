package de.ag.sqala.drivers

import de.ag.sqala.sql._
import java.io.Writer
import java.util.Properties
import de.ag.sqala._
import de.ag.sqala.JDBCHandle
import de.ag.sqala.sql.View
import de.ag.sqala.relational.Schema

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

  def domainValue(domain:Domain, value:AnyRef): String = domain match {
    case Domain.String | Domain.BoundedString(_) => "'%s'".format(value)
    case Domain.IdentityInteger => "default"
    case Domain.Integer => value.toString
    case Domain.Double => value.toString
    case Domain.Nullable(nDomain) => if (value == null) "null" else domainValue(nDomain, value)
    case Domain.Boolean => ???
    case Domain.Blob => ???
    case Domain.CalendarTime => ???
    case Domain.Set(_) => ???
    case Domain.Product(_) => ???
  }

  def insert(tableName: View.TableName, schema: Schema, values: Seq[AnyRef]): Int = {
    val sql = "INSERT INTO \"%s\"(%s) VALUES (%s)".format(
      tableName,
      schema.attributes.mkString(", "),
      schema.domains.zip(values).map{case dv => domainValue(dv._1, dv._2)}.mkString(", ")
    )
    val statement = connection.createStatement()
    val result = statement.executeUpdate(sql)
    statement.close()
    result
  }

  def delete(tableName: View.TableName, condition: Expr): Int = {
    val sql = "DELETE FROM \"%s\" WHERE %s".format(tableName, condition.toString(sqlWriteParameterization))
    val statement = connection.createStatement()
    val result = statement.executeUpdate(sql)
    statement.close()
    result
  }


  def update(tableName: View.TableName, condition: Expr, updates: Seq[(View.ColumnName, Expr)]): Int = {
    val clauses = updates.map {
      case (columnName, value) => "%s = %s".format(columnName, value.toString(sqlWriteParameterization))
    }
    val sql = "UPDATE \"%s\" SET %s WHERE %s".format(
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

  def domain2Db2Domain(domain: Domain): String = domain match {
    case Domain.String => "VARCHAR(32672)" // 32672 is the max (for v10); use BoundedString to define max yourself
    case Domain.BoundedString(maxSize) => "VARCHAR(%d)".format(maxSize)
    case Domain.IdentityInteger => "INT NOT NULL GENERATED ALWAYS AS IDENTITY(START WITH 1, INCREMENT BY 1, NO CYCLE)"
    case Domain.Integer => "INTEGER"
    case Domain.Double => "DOUBLE"
    case Domain.Blob => "BLOB(1M)" // 1M is the default (for v10); FIXME allow specifying max
    case Domain.CalendarTime => "TIMESTAMP(6)" // 6 fractional seconds digits is the default (for v10); FIXME allow specifiying precision
    case _ => throw new RuntimeException("not implemented")
  }

  def schemaToDDTList(schema: Schema): Seq[String] =
    schema.schema.map{
      case(attr, domain) => "%s %s".format(attr, domain2Db2Domain(domain))
    }

  def createTable(name: View.TableName, schema: Schema) {
    val sql = "CREATE TABLE \"%s\"(\n%s\n)".format(name, schemaToDDTList(schema).mkString(",\n"))
    val statement = connection.createStatement()
    statement.execute(sql)
    statement.close()
  }

  def dropTable(name: View.TableName) {
    val statement = connection.createStatement()
    statement.execute("DROP TABLE \"%s\"".format(name))
    statement.close()
  }

  def dropTableIfExists(name: View.TableName) {
    val statement = connection.createStatement()
    val sql = "SELECT tabname FROM syscat.tables WHERE tabschema=(SELECT current_schema FROM sysibm.sysdummy1) and tabname='%s'".format(name)
    if (statement.execute(sql)) {
      val rs = statement.getResultSet
      if(rs.next()) {
        val statement2 = connection.createStatement()
        statement2.execute("DROP TABLE \"%s\"".format(name))
        statement2.close()
      }
      statement.close()
    } else {
       throw new RuntimeException("unexpectedly received update count")
    }
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
