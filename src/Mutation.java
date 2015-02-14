import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.*;
import com.mysql.jdbc.DatabaseMetaData;
import com.mysql.jdbc.ResultSetMetaData;


public class Mutation {

	  public static void main(String args[]) {

	  Connection con = null; 

	  Statement st = null; 

	  ResultSet rs = null; 



	 final String url = "jdbc:mysql://localhost:3306/"; 

	 final String db = "I1"; 

	 final String driver = "com.mysql.jdbc.Driver"; 

	 final String user = "root"; 

	 final String pass = "root"; 



	  try { 
		

	  Class.forName(driver);

	  con = DriverManager.getConnection(url + db, user, pass);

	  st = con.createStatement();
	  
	  DatabaseMetaData md = (DatabaseMetaData) con.getMetaData();
	  ResultSet rsTable = md.getTables(null, null, "%", null);
	 
	  while (rsTable.next()) {

		ResultSet rsP=  md.getPrimaryKeys(null, null, rsTable.getString(3));
		ResultSet rsF = md.getImportedKeys(null, null, rsTable.getString(3));
	    String sql = "select * from "+rsTable.getString(3); 
	   

		  rs = st.executeQuery(sql);

		  ResultSetMetaData metaData = (ResultSetMetaData) rs.getMetaData();



		  int rowCount = metaData.getColumnCount();


		  System.out.println();
		  System.out.println("Table Name : " + metaData.getTableName(2));
		   while (rsP.next()) {
			      String columnName = rsP.getString("COLUMN_NAME");
			      System.out.println("getPrimaryKeys(): columnName=" + columnName);
			    }
		   while (rsF.next()) {
		        String fkTableName = rsF.getString("FKTABLE_NAME");
		        String fkColumnName = rsF.getString("FKCOLUMN_NAME");
		        String pkTableName = rsF.getString("PKTABLE_NAME");
		        String pkColumnName = rsF.getString("PKCOLUMN_NAME");
		        short updateRule = rsF.getShort("UPDATE_RULE");
		        System.out.println(fkTableName + "." + fkColumnName + " -> " + pkTableName + "." + pkColumnName+":"+updateRule);
		    }

		  System.out.println("Field  \tsize\tDataType");



		  for (int i = 0; i < rowCount; i++) {

		  System.out.print(metaData.getColumnName(i + 1) + "  \t");

		  System.out.print(metaData.getColumnDisplaySize(i + 1) + "\t");

		  System.out.println(metaData.getColumnTypeName(i + 1));
		  

		  } 
		  
	  }
	  createnewdatabase();
	  populatedatabase();
	 // compare();

	  } catch (Exception e) {

	  System.out.println(e);

	  } 

	  } 


	  public static void createnewdatabase() throws ClassNotFoundException, SQLException{
		  
		  final String url = "jdbc:mysql://localhost:3306/"; 

		  final String db = "I1"; 

		  final String driver = "com.mysql.jdbc.Driver"; 

		  final String user = "root"; 

		  final String pass = "root"; 
		  Connection con = null; 

		  Statement st = null; 

		  ResultSet rs = null; 
		  
		  Class.forName(driver);

		  con = DriverManager.getConnection(url + db, user, pass);

		  st = con.createStatement();
		  
		  DatabaseMetaData md = (DatabaseMetaData) con.getMetaData();
		  ResultSet rsTable = md.getTables(null, null, "%", null);
		  String sqldrop = "drop schema Mutationtest";
		  st.execute( sqldrop );
		  String sqlnew = "create database Mutationtest";
		  st.execute( sqlnew);
		  //sqlnew = "use Mutationtest";
		  //st.execute( sqlnew );
		  String sqlcreate;
		  String sqluse;
		 
		  while (rsTable.next()) {
			  System.out.println(rsTable.getString(3));
			  int i;
			  sqluse="use I1";
			  st.execute(sqluse);
			  String sql = "select * from "+rsTable.getString(3);
			  rs = st.executeQuery(sql);
			  while(rs.next())
			  {
				  System.out.println();
				  break;
			  }

			  ResultSetMetaData metaData = (ResultSetMetaData) rs.getMetaData();
			  int rowCount = metaData.getColumnCount();
			  System.out.println(rowCount);
			  	sqluse="use Mutationtest";
				st.execute(sqluse);
				sqlcreate = "create table "+rsTable.getString(3)+"\n"+"("; 
		
				for(i=0;i<rowCount;i++)
				{
					if(i!=rowCount-1)
						sqlcreate = sqlcreate+ metaData.getColumnName(i + 1)+" "+metaData.getColumnTypeName(i + 1)+"("+metaData.getColumnDisplaySize(i + 1)+"),\n";
					else
						sqlcreate = sqlcreate+ metaData.getColumnName(i + 1)+" "+metaData.getColumnTypeName(i + 1)+"("+metaData.getColumnDisplaySize(i + 1)+")";
					 
				}
				sqlcreate=sqlcreate+");\n";
				
						/*"(ID varchar(5) not null,\n" +
						"name varchar(20) not null,\n" +
						"deptName varchar(20) not null,\n"+
						"salary numeric(8,2) not null,\n"+
						"primary key (ID));\n";*/
				st.execute( sqlcreate );
				/*sqlcreate = "create table teaches\n"+
						"(ID varchar(5) not null,\n"+
						"courseId varchar(8) not null,\n"+
						"semester varchar(6) not null,\n"+
						"year numeric(4,0) not null,\n"+
						"primary key (ID, courseId, semester, year),\n"+
						"foreign key (ID) references instructor(ID)\n"+
						"on delete cascade);\n";
				st.execute( sqlcreate );*/
			  
		  }
		  con.close();
	  }
 public static void populatedatabase() throws ClassNotFoundException, SQLException{
		  
		  final String url = "jdbc:mysql://localhost:3306/"; 

		  final String db = "I1"; 

		  final String driver = "com.mysql.jdbc.Driver"; 

		  final String user = "root"; 

		  final String pass = "root"; 
		  String sqlinsert;
		  Connection con = null; 

		  Statement st = null; 
		  Statement st2 = null;

		  ResultSet rs = null; 
		  
		  Class.forName(driver);

		  con = DriverManager.getConnection(url + db, user, pass);

		  st = con.createStatement();
		  st2= con.createStatement();
		  DatabaseMetaData md = (DatabaseMetaData) con.getMetaData();
		  ResultSet rsTable = md.getTables(null, null, "%", null);
		  String sqluse= "use mutationtest";
		  st.executeQuery(sqluse);
		  while(rsTable.next())
		  {
			  String temp;
			  int i;
			  String sql = "select * from "+rsTable.getString(3);
			  sqluse= "use I1";
			  st.execute(sqluse);
			  rs = st.executeQuery(sql);
			  
			  ResultSetMetaData metaData = (ResultSetMetaData) rs.getMetaData();
			  int rowCount = metaData.getColumnCount();
			  while(rs.next())
			  {
				 sqlinsert="insert into "+rsTable.getString(3)+ " "+"values (";
				 for(i=0;i<rowCount;i++)
				 {
					 if(metaData.getColumnTypeName(i + 1)=="VARCHAR")
					 { 
						
						if(i!=rowCount-1)
							sqlinsert=sqlinsert+"'"+rs.getString(i+1)+"'"+",";
						else
							sqlinsert=sqlinsert+"'"+rs.getString(i+1)+"'"+")";
						
					 }
					 else
					 {
						 if(i!=rowCount-1)
								sqlinsert=sqlinsert+rs.getString(i+1)+",";
							else
								sqlinsert=sqlinsert+rs.getString(i+1)+")";
					 }
				 }
				 //System.out.println(sqlinsert);
				 sqluse= "use mutationtest";
				 st2.execute(sqluse);
				 st2.execute(sqlinsert);
				 sqluse= "use I1";
				 st2.execute(sqluse);
				 //System.out.println("hello");
				 //System.out.println(rs.getString(1));
				// System.out.println("hello");
			  }
			  
		  }
		  //con.close();	  
		  }
 
 public static void compare() throws ClassNotFoundException, SQLException{

	  final String url = "jdbc:mysql://localhost:3306/"; 

	  final String db = "I1"; 
	  final String db2= "mutationtest";

	  final String driver = "com.mysql.jdbc.Driver"; 

	  final String user = "root"; 

	  final String pass = "root"; 
	  Connection con = null; 
	  Connection con2 =null;

	  Statement st = null; 
	  Statement st2 = null;

	  ResultSet rs = null; 
	  ResultSet rs2= null;
	  
	  Class.forName(driver);

	  con = DriverManager.getConnection(url + db, user, pass);
	  con2= DriverManager.getConnection(url + db2, user, pass);

	  st = con.createStatement();
	  st2= con2.createStatement();
	  DatabaseMetaData md = (DatabaseMetaData) con.getMetaData();
	  ResultSet rsTable = md.getTables(null, null, "%", null);
	  DatabaseMetaData md2 = (DatabaseMetaData) con2.getMetaData();
	  ResultSet rsTable2 = md2.getTables(null, null, "%", null);
	  System.out.println("hello");
	  //String sqluse= "use mutationtest";
	  //st.executeQuery(sqluse);
	  while(rsTable.next())
	  {
		 int count=0;
		  int i;
		  System.out.println("hello");
		  String sql = "select * from "+rsTable.getString(3);
		  //sqluse= "use I1";
		  //st.execute(sqluse);
		 // System.out.println("hello");
		  rs = st.executeQuery(sql);
		  ResultSetMetaData metaData = (ResultSetMetaData) rs.getMetaData();
		  int rowCount = metaData.getColumnCount();
		  while(rsTable2.next())
		  {
			 // System.out.println("hello");
			  String sql2= "select * from "+rsTable2.getString(3);
			  //System.out.println(rsTable2.getString(3));
			  rs2=st2.executeQuery(sql2);
			  rs2.next();
			  while(rs.next())
			  {
				  
				  
					  for(i=0;i<rowCount;i++)
					  {
						  System.out.println("********");
						  System.out.println(rs.getString(i+1));
						  System.out.println(rs2.getString(i+1));
						  System.out.println("********");
						  if( rs.getString(i+1)==rs2.getString(i+1))
							  count++;
					
				  }
					  rs2.next();
				  }
			  System.out.println(count); 
				  
			  }
		  }
		  //System.out.println(count);
	  }
	 
 }


