package de.ag.sqala.test.jdbc

import java.math.BigDecimal
import java.sql._

/**
 *
 */
object Db2JDBCTest extends App {
  System.out.println("Hello world!")

  val (host, port) = args.size match {
    case 0 => ("192.168.1.138", "50001")
    case 2 => (args(0), args(1))
    case _ => System.err.println("cmdline args missing: host port")
      System.exit(1)
      ("","") //keep type inference happy
  }

  // make configurable later, if necessary
  val user = "db2inst2"
  val password = "db2inst2"
  testDb2Sample(host, port, user, password)

  def testDb2Sample(host: String, port: String, user: String, pass: String) {
    val database: String = "sample"
    var connection: Connection = null
    try {
      Class.forName("com.ibm.db2.jcc.DB2Driver")
      connection = DriverManager.getConnection("jdbc:db2://" + host + ":" + port + "/" + database, user, pass)
      val preparedStatement: PreparedStatement = connection.prepareStatement("SELECT * FROM STAFF WHERE DEPT = ?")
      preparedStatement.setInt(1, 20)
      val resultSet: ResultSet = preparedStatement.executeQuery
      val strFormat: String = "%-5s %-12s %-5s %-5s %-5s %-10s %-10s"
      val header: String = String.format(strFormat, "id", "name", "dept", "job", "years", "salary", "comm")
      System.out.println(header)
      System.out.println(String.format(strFormat, "-----", "------------", "-----", "-----", "-----", "----------", "----------"))
      while (resultSet.next) {
        val id: Int = resultSet.getInt("ID")
        val name: String = resultSet.getString("NAME")
        val dept: Int = resultSet.getInt("DEPT")
        val job: String = resultSet.getString("JOB")
        val years: Int = resultSet.getInt("YEARS")
        val salary: BigDecimal = resultSet.getBigDecimal("SALARY")
        val comm: BigDecimal = resultSet.getBigDecimal("COMM")
        val out: String = "%5d %-12s %5d %5s %5d %10.2f %10.2f".format(id, name, dept, job, years, salary, comm)
        System.out.println(out)
      }
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    finally {
      if (connection != null) {
        try {
          connection.close()
        }
        catch {
          case e: SQLException => {
            e.printStackTrace()
          }
        }
      }
    }
  }
}