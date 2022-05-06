package com.databricks.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DatabricksSQLRead {
    private Connection connection;
    private PreparedStatement stmt = null;
    private ResultSet readResultSet = null;

    public static void main(String[] args) {
        new DatabricksSQLRead().readUnicodeData();
    }

    public void readUnicodeData() {
        try {
            String connectionUrl = "*****";
            Class.forName("com.simba.spark.jdbc.driver");
            connection = DriverManager.getConnection(connectionUrl);

            System.out.println("reading data from Databricks");
            DatabaseMetaData dbMetadata = connection.getMetaData();
            ResultSet result = dbMetadata.getColumns(null, "default", "unicodesql", null);
            int count = 0;
            while (result.next()) {
                count++;
            }
            Properties properties = System.getProperties();
            properties.setProperty("charSet", "UTF-16");

            String readQuery = "select * from unicodesql";
            stmt = connection.prepareStatement(readQuery);
            readResultSet = stmt.executeQuery();
            while (readResultSet.next()) {
                List<Object> rowData = new ArrayList<>();
                for (int i = 1; i <= count; i++) {
                    rowData.add(readResultSet.getString(i));
                }
            }
        } catch (Exception ex) {
            System.out.println(ex);
        }
    }

}
