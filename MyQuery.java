/*****************************
Query the University Database
*****************************/
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.util.*;
import java.lang.String;
import java.sql.Types;
import java.util.Scanner;

public class MyQuery {

    private Connection conn = null;
	 private Statement statement = null;
	 private ResultSet resultSet = null;
    
    public MyQuery(Connection c)throws SQLException
    {
        conn = c;
        // Statements allow to issue SQL queries to the database
        statement = conn.createStatement();
    }
    
    public void findFall2009Students() throws SQLException
    {
        String query  = "select distinct name from student natural join takes where semester = \'Fall\' and year = 2009;";

        resultSet = statement.executeQuery(query);
    }
    
    public void printFall2009Students() throws IOException, SQLException
    {
	      System.out.println("******** Query 0 ********");
         System.out.println("name");
         while (resultSet.next()) {
			// It is possible to get the columns via name
			// also possible to get the columns via the column number which starts at 1
			String name = resultSet.getString(1);
         System.out.println(name);
   		}        
    }

    public void findGPAInfo() throws SQLException
    {
        String query  = "SELECT\n"+
        "t.ID, s.name, FORMAT((SUM(\n"+
        "CASE t.grade\n"+
            "WHEN 'A' THEN 4.0\n"+
            "WHEN 'A-' THEN 3.7\n"+
            "WHEN 'B+' THEN 3.3\n"+
            "WHEN 'B' THEN 3.0\n"+
            "WHEN 'B-' THEN 2.7\n"+
            "WHEN 'C+' THEN 2.3\n"+
            "WHEN 'C' THEN 2.0\n"+
            "WHEN 'C-' THEN 1.7\n"+
            "WHEN 'D+' THEN 1.3\n"+
            "WHEN 'D' THEN 1.0\n"+
            "WHEN 'D-' THEN 0.7\n"+
            "WHEN 'F' THEN 0\n"+
        "END * c.credits)/SUM(c.credits)), 2) AS GPA\n"+
    "FROM\n"+
        "student s JOIN takes t ON s.ID = t.ID\n"+
        "JOIN course c ON t.course_id = c.course_id\n"+
    "WHERE\n"+
         "t.grade <> 'F'\n"+
    "GROUP BY\n"+
        "s.ID;"
    ;

        resultSet = statement.executeQuery(query);
    }
    
    public void printGPAInfo() throws IOException, SQLException
    {
		   System.out.println("******** Query 1 ********");
           while(resultSet.next())
           {
            String ID = resultSet.getString(1);
            String name = resultSet.getString(2);
            String GPA = resultSet.getString(3);
            System.out.println(String.format("%s %s %s", ID, name, GPA));
           }
    }

    public void findMorningCourses() throws SQLException
    {
        String query  = "SELECT DISTINCT\n" +
        "c.course_id, s.sec_id, c.title, s.semester, s.year, i.name, count(takes.course_id) as enrollment\n" +
    "FROM\n" +
        "section s\n" + 
        "INNER JOIN takes on s.course_id = takes.course_id\n" +
        "INNER JOIN course c on c.course_id = takes.course_id\n" +
        "INNER JOIN time_slot ts on ts.time_slot_id = s.time_slot_id\n" +
        "INNER JOIN teaches on teaches.course_id = takes.course_id\n" +
        "INNER JOIN instructor i on i.ID = teaches.ID\n" +
    "WHERE\n" +
         "ts.start_hr < 12\n" +
    "GROUP BY\n" +
        "c.course_id, s.sec_id, c.title, s.semester, s.year, i.name;";

        resultSet = statement.executeQuery(query);
    }

    public void printMorningCourses() throws IOException, SQLException
    {
	   	System.out.println("******** Query 2 ********");
           while(resultSet.next())
           {
            String cid = resultSet.getString(1);
            String sid = resultSet.getString(2);
            String title = resultSet.getString(3);
            String semester = resultSet.getString(4);
            String year = resultSet.getString(5);
            String name = resultSet.getString(6);
            String enrollment = resultSet.getString(7);
            System.out.println(String.format("%s\t %s\t %s %s %s %s %s", cid, sid, title, semester, year, name, enrollment));
           }
    }

    public void findBusyClassroom() throws SQLException
    {
        String query = "SELECT\n" +
        "s.building, s.room_number, COUNT(s.building) as frequency\n" +
        "FROM\n" +
        "section s\n" +
        "GROUP BY\n" +
        "s.building, s.room_number\n" +
        "HAVING\n" +
        "COUNT(s.building) = (SELECT MAX(frequency) FROM (SELECT s2.building, COUNT(s2.building) frequency\n" +
        "FROM section s2 GROUP BY s2.building, s2.room_number) as frequency);";

        resultSet = statement.executeQuery(query);
    }

    public void printBusyClassroom() throws IOException, SQLException
    {
		   System.out.println("******** Query 3 ********");
           while(resultSet.next())
           {
            String building = resultSet.getString(1);
            String room_number = resultSet.getString(2);
            String frequency = resultSet.getString(3);
            System.out.println(String.format("%s %s %s", building, room_number, frequency));
           }
    }

    public void findPrereq() throws SQLException
    {
        String query = "SELECT\n"+
        "c.title as course,\n"+ 
        "CASE\n"+ 
            "WHEN\n"+ 
                "c.course_id IN (SELECT course_id FROM prereq)\n"+
            "THEN\n"+ 
                "(SELECT c2.title\n"+ 
                "FROM course c2, prereq p\n"+
                "WHERE c.course_id = p.course_id\n"+
                "AND c2.course_id = p.prereq_id)\n"+
            "ELSE 'N/A'\n"+
        "END AS prereq FROM course c;\n";
        
        resultSet = statement.executeQuery(query);
    }

    public void printPrereq() throws IOException, SQLException
    {
		   System.out.println("******** Query 4 ********");

           while(resultSet.next())
           {
            String course = resultSet.getString(1);
            String prereq = resultSet.getString(2);
            System.out.println(String.format("%s\t\t\t%s", course, prereq));
           }
    }

    public void updateTable() throws SQLException
    {
        String query ="UPDATE studentCopy sc\n"+
        "SET tot_cred = (SELECT SUM(credits)\n"+
                        "FROM course NATURAL JOIN takes t\n"+
                        "WHERE grade <> 'F' AND grade is not null\n"+
                        "AND sc.ID = t.ID);";
        statement.execute(query);
        String print = "SELECT * FROM studentCopy";
        resultSet = statement.executeQuery(print);
    }

    public void printUpdatedTable() throws IOException, SQLException
    {
		   System.out.println("******** Query 5 ********");
           while(resultSet.next())
           {
            String ID = resultSet.getString(1);
            String name = resultSet.getString(2);
            String dept_name = resultSet.getString(3);
            String tot_cred = resultSet.getString(4);
            System.out.println(String.format("%s\t %s\t %s %s", ID, name, dept_name, tot_cred));
           }
    }
	
	 public void findDeptInfo() throws SQLException
	 {
		  System.out.println("******** Query 6 ********");

          Scanner sc = new Scanner(System.in);
          System.out.println("Enter department name: ");
          String str = sc.nextLine();
          CallableStatement stmt = conn.prepareCall("{CALL deptInfo(?, ?, ?, ?)}");
          //set string
          stmt.setString(1, str);
          //Register out parameters using types
          stmt.registerOutParameter(2, Types.INTEGER);
          stmt.registerOutParameter(3, Types.INTEGER);
          stmt.registerOutParameter(4, Types.INTEGER);

          stmt.executeQuery();
          //Assign values registered in last step
          int instructors = stmt.getInt(2);
          int salary = stmt.getInt(3);
          int budget = stmt.getInt(4);

          System.out.println(str + " department has " + instructors + " instructors.");
          System.out.println(str + " department has a total salary of $" + salary + ".0");
          System.out.println(str + " department has a " + budget + ".0");
	 }
    
    
    public void findFirstLastSemester() throws SQLException
    {
        String query = "SELECT DISTINCT\n"+  
        "s.ID, s.name, (SELECT DISTINCT(SELECT(MIN(semester))\n"+ 
                                       "FROM takes t1\n"+ 
                                       "WHERE t.ID = t1.ID\n"+
                                       "AND year = (SELECT MIN(year)\n"+
                                                   "FROM takes t2\n"+
                                                   "WHERE t1.ID = t2.ID))) AS first_term,\n"+
                      "(SELECT DISTINCT(SELECT(MIN(year))\n"+ 
                                       "FROM takes t3\n"+
                                       "WHERE t.ID = t3.ID))\n"+
                                       "AS first_year,\n"+   
                      "(SELECT DISTINCT(SELECT(MAX(semester))\n"+ 
                                       "FROM takes t4\n"+
                                       "WHERE t.ID = t4.ID\n"+
                                       "AND year = (SELECT MAX(year)\n"+
                                                   "FROM takes t5\n"+
                                                   "WHERE t4.ID = t5.ID))) AS last_term,\n"+                 
                      "(SELECT DISTINCT(SELECT(MAX(year))\n"+
                                       "FROM takes t6\n"+
                                       "WHERE t.ID = t6.ID))\n"+
                                       "AS last_year\n"+
        "FROM  takes t JOIN student s\n"+
        "WHERE t.ID = s.ID\n"+
        "GROUP BY s.ID;";
    	resultSet = statement.executeQuery(query);
    }

    public void printFirstLastSemester() throws IOException, SQLException
    {
        System.out.println("******** Query 7 ********");
        while (resultSet.next())
        {
        	String ID = resultSet.getString(1);
        	String Name = resultSet.getString(2);
        	String First_Term = resultSet.getString(3);
        	String First_Year = resultSet.getString(4);
            String Last_Term = resultSet.getString(5);
            String Last_Year = resultSet.getString(6);

        	System.out.println(String.format("%s %s %s %s %s %s", ID, Name, First_Term, First_Year, Last_Term, Last_Year));
        }
    }

}