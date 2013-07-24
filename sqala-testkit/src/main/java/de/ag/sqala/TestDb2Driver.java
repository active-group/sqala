package de.ag.sqala;

import java.math.BigDecimal;
import java.sql.*;


/**
 *
 */
public class TestDb2Driver {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        testDb2Sample("192.168.1.138", "50001", "db2inst2", "db2inst2");
    }

    public static void testDb2Sample(String host, String port, String user, String pass) {
        String database = "sample";
        Connection connection = null;
        try
        {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
            connection = DriverManager.getConnection("jdbc:db2://" + host + ":" + port + "/" + database, user, pass);

            PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM STAFF WHERE DEPT = ?");
            preparedStatement.setInt(1,20);

            ResultSet resultSet = preparedStatement.executeQuery();
            String strFormat = "%-5s %-12s %-5s %-5s %-5s %-10s %-10s";
            String header = String.format(strFormat,
                    "id", "name", "dept", "job", "years", "salary", "comm");
            System.out.println(header);
            System.out.println(String.format(strFormat, "-----", "------------", "-----", "-----", "-----", "----------", "----------"));
            while(resultSet.next()) {
                int id = resultSet.getInt("ID");
                String name = resultSet.getString("NAME");
                int dept = resultSet.getInt("DEPT");
                String job = resultSet.getString("JOB");
                int years = resultSet.getInt("YEARS");
                BigDecimal salary = resultSet.getBigDecimal("SALARY");
                BigDecimal comm = resultSet.getBigDecimal("COMM");

                String out = String.format("%5d %-12s %5d %5s %5d %10.2f %10.2f",
                        id, name, dept, job, years, salary, comm);
                System.out.println(out);
            }


        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }


    }
}
