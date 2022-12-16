package com.databricks.spark

import java.sql.DriverManager
import java.util.Properties

object JdbcReadTest {

  def main(args: Array[String]): Unit = {

    // serverless java.sql.SQLException: [Databricks][DatabricksJDBCDriver](500593) Communication link failure. Failed to connect to server. Reason: HTTP Response code: 500, Error message: BAD_REQUEST: Invalid access to a Virtual Cluster, 1129-161124-e42e2pi5.
   Class.forName("com.databricks.client.jdbc.Driver")
    def readData(): Unit = {
      val http_path = "sql/protocolv1/o/6935536957980197/1215-144556-ed60jkre"
      val url = s"jdbc:databricks://adb-6935536957980197.17.azuredatabricks.net:443/default;transportMode=http;ssl=1;HttpPath=${http_path}"
      val prop = new Properties()
      val token = "dap***"
      prop.put("PWD", token)

      try {
        val connection = DriverManager.getConnection(url, prop)
        System.out.println("reading data from Databricks")
        val query = "SELECT id FROM sandeepk.customers"
        val stmt = connection.prepareStatement(query)
        val resultSet = stmt.executeQuery
        while (resultSet.next()) {
          val data = resultSet.getInt(1)
          println(s"data is ${data} ")
        }
        connection.close()
      } catch {
        case ex: Exception =>
          System.out.println(ex)
      }
    }
    readData()

  }
}
