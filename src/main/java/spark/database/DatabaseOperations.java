package spark.database;

import java.util.List;
import scala.Tuple2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseOperations {
    
    static Connection connection = null; 
    static PreparedStatement stmt = null;

     public static void insertIntoDatabase(List<Tuple2<String, Integer>> list, String table) throws SQLException {

        connection = DatabaseConnector.getConnection();

        for (Tuple2<String, Integer> tuple : list) {
            String sql = "INSERT INTO " + table + " " + "VALUES(?,?)";
            stmt = connection.prepareStatement(sql);
            stmt.setString(1, tuple._1);
            stmt.setInt(2, tuple._2);
            stmt.executeUpdate();
            
        }

        closeResources();
    }

    public static void insertIntoDatabase(List<Tuple2<Integer, Integer>> list, String table, boolean b)
            throws SQLException {

        connection = DatabaseConnector.getConnection();

        for (Tuple2<Integer, Integer> tuple : list) {
            String sql = "INSERT INTO " + table + " " + "VALUES(?,?)";
            stmt = connection.prepareStatement(sql);
            stmt.setInt(1, tuple._1);
            stmt.setInt(2, tuple._2);
            stmt.executeUpdate();
        }

        closeResources();
    }

    public static String printDatabase(String table, String tupleType) throws SQLException {
        connection = DatabaseConnector.getConnection();
        StringBuffer string = new StringBuffer();
        String sql = "SELECT * FROM " + table; // Our SQL query
        stmt = connection.prepareStatement(sql); // Creates the prepared statement from the query
        ResultSet rs = stmt.executeQuery(); // Queries the database

        string.append("Printing " + table + "\n");
        while (rs.next()) {
            if (tupleType.toLowerCase().equals("string"))
                string.append(rs.getString(1) + " " + rs.getInt(2) + "\n");
            else if (tupleType.toLowerCase().equals("integer"))
                string.append(rs.getInt(1) + " " + rs.getInt(2) + "\n");
        }
        closeResources();
        return string.toString();

    }



    public static int readFromDatabase(String table, String keyValue) throws SQLException {
        connection = DatabaseConnector.getConnection();
        String sql = "SELECT cases FROM " + table + " WHERE keyType=?";
        stmt = connection.prepareStatement(sql);
        if(table.equals("byMonthTable"))
            stmt.setInt(1, Integer.parseInt(keyValue));
        else
            stmt.setString(1, keyValue);

        ResultSet rs = stmt.executeQuery();
        rs.next();
        return rs.getInt(1);
    }

    private static void closeResources() {

		try {
			if (stmt != null)
				stmt.close();
		} catch (SQLException e) {
			System.out.println("Could not close statement!");
			e.printStackTrace();
		}

		try {
			if (connection != null)
				connection.close();
		} catch (SQLException e) {
			System.out.println("Could not close connection!");
			e.printStackTrace();
		}
	}
}