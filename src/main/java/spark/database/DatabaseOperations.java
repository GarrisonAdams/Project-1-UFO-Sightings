package spark.database;

import java.util.List;
import scala.Tuple2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabaseOperations {
    
    Connection connection = null; 
    PreparedStatement stmt = null;

    public void insertIntoDatabase(List<Tuple2<String, Integer>> list, String table) throws SQLException {

        connection = DatabaseConnector.getConnection();

        for (Tuple2<String, Integer> tuple : list) {
            String sql = "INSERT INTO table VALUES(?,?)";
            stmt = connection.prepareStatement(sql);
            stmt.setString(1, tuple._1);
            stmt.setInt(2, tuple._2);
            stmt.addBatch();
        }

        stmt.executeBatch();
        closeResources();
    }

    public void insertIntoDatabase(List<Tuple2<Integer, Integer>> list, String table,boolean b) throws SQLException {

        connection = DatabaseConnector.getConnection();

        for (Tuple2<Integer, Integer> tuple : list) {
            String sql = "INSERT INTO table VALUES(?,?)";
            stmt = connection.prepareStatement(sql);
            stmt.setInt(1, tuple._1);
            stmt.setInt(2, tuple._2);
            stmt.addBatch();
        }

        stmt.executeBatch();
        closeResources();
    }

    public String readFromDatabase(String table, String tupleType) throws SQLException {
        connection = DatabaseConnector.getConnection();
        StringBuffer string = new StringBuffer();

        String sql = "SELECT * FROM ?"; // Our SQL query
        stmt = connection.prepareStatement(sql); // Creates the prepared statement from the query
        stmt.setString(1, table);
        ResultSet rs = stmt.executeQuery(); // Queries the database
        
        while(rs.next())
        {
            if(tupleType.toLowerCase().equals("string"))
                string.append(rs.getString(0) + " " + rs.getInt(1) + "\n");
            else if(tupleType.toLowerCase().equals("integer"))
                string.append(rs.getString(0) + " " + rs.getInt(1) + "\n");
        }
        closeResources();
        return string.toString();

    }

    
	private void closeResources() {

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