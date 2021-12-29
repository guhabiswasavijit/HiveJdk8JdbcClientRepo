package self.demo;
import java.io.File;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class HiveJdbcClient {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	public static void main(String[] args) throws Exception{
		  Class.forName(driverName);
    
	      Connection con = DriverManager.getConnection("jdbc:hive2://127.0.0.1:8081/userdb", "", "");
	      Statement stmt = con.createStatement();	      
	      stmt.execute("CREATE TABLE IF NOT EXISTS "
	    	         +" employee ( eid int, name String, "
	    	         +" salary String, destignation String)"
	    	         +" COMMENT 'Employee details'"
	    	         +" ROW FORMAT DELIMITED"
	    	         +" FIELDS TERMINATED BY '\t'"
	    	         +" LINES TERMINATED BY '\n'"
	    	         +" STORED AS TEXTFILE");

	      ResultSet rs = stmt.executeQuery("SELECT * FROM employee");
	      while(rs.next()) {
	    	  System.out.println(rs.getInt("eid"));
	    	  System.out.println(rs.getString("name"));
	    	  System.out.println(rs.getString("salary"));
	    	  System.out.println(rs.getString("destignation"));
	      }
	      rs.close();
	      stmt.close();
	      con.close();

	}
}


