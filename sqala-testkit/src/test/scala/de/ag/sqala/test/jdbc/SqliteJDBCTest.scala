package de.ag.sqala.test.jdbc

import java.sql._

// example from https://bitbucket.org/xerial/sqlite-jdbc

object SqliteJDBCTest extends App {

  // load the sqlite-JDBC driver using the current class loader
  try {
    Class.forName("org.sqlite.JDBC")
  } catch {
    case e:ClassNotFoundException =>
      System.err.println("sqlite jdbc driver not found: " + e.getMessage)
      System.exit(1)
  }

  var connection: Connection = _
  try {
    // create a database connection
    connection = DriverManager.getConnection("jdbc:sqlite::memory:")
    val statement = connection.createStatement()
    statement.setQueryTimeout(30) // set timeout to 30 sec.

    statement.executeUpdate("drop table if exists person")
    statement.executeUpdate("create table person (id integer, name string)")
    statement.executeUpdate("insert into person values(1, 'leo')")
    statement.executeUpdate("insert into person values(2, 'yui')")
    val rs = statement.executeQuery("select * from person")
    while (rs.next()) {
      // read the result set
      System.out.println("name = " + rs.getString("name"))
      System.out.println("id = " + rs.getInt("id"))
    }
  } catch {
    case e: SQLException =>
      // if the error message is "out of memory",
      // it probably means no database file is found
      System.err.println(e.getMessage)
  } finally {
    try {
      if (connection != null)
        connection.close()
    } catch {
      case e: SQLException =>
        // connection close failed.
        System.err.println(e)
    }
  }
}
