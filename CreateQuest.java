import java.util.*;
import java.net.*;
import java.text.*;
import java.lang.*;
import java.io.*;
import java.sql.*;
import java.sql.Date;

import pgpass.*;

public class CreateQuest{
  private Connection conDB;        // Connection to the database system.
  private String url;              // URL: Which database?
  private String user = "liaot3"; // Database user account
  private int counter=1;
  private Date date;
  private String realm;
  private String theme;
  private String database;
  private Integer amount ;
  private Double seed=0.0;
  private int sum;
  public CreateQuest (String[] args) {

      // Set up the DB connection.
      try {
          // Register the driver with DriverManager.
          Class.forName("org.postgresql.Driver").newInstance();
      } catch (ClassNotFoundException e) {
          e.printStackTrace();
          System.exit(0);
      } catch (InstantiationException e) {
          e.printStackTrace();
          System.exit(0);
      } catch (IllegalAccessException e) {
          e.printStackTrace();
          System.exit(0);
      }

      url = "jdbc:postgresql://db:5432/";

      try {

          date = Date.valueOf(args[0]);
          realm = new String(args[1]);
          theme = new String(args[2]);
          amount = new Integer(args[3]);
          if(args.length>4)
          user = new String(args[4]);
          if(args.length>5)
          seed = new Double(args[5]);

      } catch (NumberFormatException e) {
          System.out.println("exception");

          System.exit(0);
      }

      // set up acct info
      // fetch the PASSWD from <.pgpass>
      Properties props = new Properties();
      try {
          String passwd = PgPass.get("db", "*", user, user);
          props.setProperty("user",    user);
          props.setProperty("password", passwd);
          // props.setProperty("ssl","true"); // NOT SUPPORTED on DB
      } catch(Exception e) {
          System.out.print("\nCould not obtain PASSWD from <.pgpass>.\n");
          System.out.println(e.toString());
          System.exit(0);
        }
      // Initialize the connection.
      try {
          // Connect with a fall-thru id & password
          //conDB = DriverManager.getConnection(url,"<username>","<password>");
          conDB = DriverManager.getConnection(url, props);
      } catch(SQLException e) {
          System.out.print("\nSQL: database connection error.\n");
          System.out.println(e.toString());
          System.exit(0);
      }



        if(!date.after(this.currentdate())) {
        	System.out.print("day is not in future");
        }
        else if(!this.realmexist()) {
        	System.out.println("realm does not exist");
        }
        else if(seed > 1 || seed <-1) {
        	System.out.println("seed value is improper");
        }
        else if(feasible()) {
        	this.insertquest();
        	this.insertloot();
        }
        else {
        	System.out.println("amount exceeds what is possible");
        }



      } //end of Constructor



      public void insertquest(){
        PreparedStatement querySt   = null;   // The query handle.
        String queryText =
            "insert into quest values(?,?,?,null)";  //theme realm day and success is null

            // Prepare the query.
            try {
                querySt = conDB.prepareStatement(queryText);
            } catch(SQLException e) {
                System.out.println("SQL#2 failed in prepare");
                System.out.println(e.toString());
                System.exit(0);
            }

            try {
                querySt.setDate(3, date);
                querySt.setString(2, realm);
                querySt.setString(1, theme);
                querySt.executeUpdate();
            } catch(SQLException e) {
                System.out.println("SQL#1 failed in execute");
                System.out.println(e.toString());
                System.exit(0);
            }



            try {
                querySt.close();
            } catch(SQLException e) {
                System.out.print("SQL#1 failed closing the handle.\n");
                System.out.println(e.toString());
                System.exit(0);
            }

      }

      public Date currentdate() {
    	  String            queryText = "";     // The SQL text.
          PreparedStatement querySt   = null;   // The query handle.
          ResultSet         answers   = null;   // A cursor.
          queryText =
        		  "select current_date ";
          try {
              querySt = conDB.prepareStatement(queryText);

          } catch(SQLException e) {
              System.out.println("SQL#1 failed in prepare");
              System.out.println(e.toString());
              System.exit(0);
          }
       // Execute the query.
          try {

              answers = querySt.executeQuery();
          } catch(SQLException e) {
              System.out.println("SQL#1 failed in execute");
              System.out.println(e.toString());
              System.exit(0);
          }

          try {
        	  if(answers.next()) {
        		 return answers.getDate(1) ;
        	  }
          }catch(SQLException e) {
              System.out.println("SQL#1 failed in cursor.");
              System.out.println(e.toString());
              System.exit(0);
          }

       // Close the cursor.
          try {
              answers.close();
          } catch(SQLException e) {
              System.out.print("SQL#1 failed closing cursor.\n");
              System.out.println(e.toString());
              System.exit(0);
          }


  		  return null;
  	  }

      //check if realm exists
      private boolean realmexist() {
    	  String            queryText = "";     // The SQL text.
          PreparedStatement querySt   = null;   // The query handle.
          ResultSet         answers   = null;   // A cursor.
          queryText =
        		  "select realm "+
        		  " from realm " +
        		  " where realm=?";
          try {
              querySt = conDB.prepareStatement(queryText);

          } catch(SQLException e) {
              System.out.println("SQL#1 failed in prepare");
              System.out.println(e.toString());
              System.exit(0);
          }

       // Execute the query.
          try {
        	  querySt.setString(1, realm);
              answers = querySt.executeQuery();
          } catch(SQLException e) {
              System.out.println("SQL#1 failed in execute");
              System.out.println(e.toString());
              System.exit(0);
          }

          try {
        	  if(answers.next()) {

        	  }
        	  else {
        		  return false;
        	  }
          }catch(SQLException e) {
              System.out.println("SQL#1 failed in cursor.");
              System.out.println(e.toString());
              System.exit(0);
          }

          // Close the cursor.
          try {
              answers.close();
          } catch(SQLException e) {
              System.out.print("SQL#1 failed closing cursor.\n");
              System.out.println(e.toString());
              System.exit(0);
          }

    	  return true;
      }

      //we have to check if it sql can satisfy amount
      private boolean feasible() {
    	  int sum_sql=0;
    	  String            queryText = "";     // The SQL text.
    	  String seedText = "";
          PreparedStatement querySt   = null;   // The query handle.
          ResultSet         answers   = null;   // A cursor.
          PreparedStatement seedstatement   = null;   // The query handle.
          ResultSet         seedset   = null;   // A cursor.

          queryText =
        		  "select * "+
        		  " from Treasure " +
        		  " order by random()";

          seedText = "select setseed (" + seed + ")";

          // Prepare the query.
          try {

        		  seedstatement = conDB.prepareStatement(seedText);


              querySt = conDB.prepareStatement(queryText);

          } catch(SQLException e) {
              System.out.println("SQL#1 failed in prepare");
              System.out.println(e.toString());
              System.exit(0);
          }

       // Execute the query.
          try {

        	  seedset= seedstatement.executeQuery();

              answers = querySt.executeQuery();
          } catch(SQLException e) {
              System.out.println("SQL#1 failed in execute");
              System.out.println(e.toString());
              System.exit(0);
          }
    	  //move the cursor
          try {

        	  if(seedset.next()) {

        	  }
        	  while(true) {
        	  if(answers.next()) {
        		  if(sum < amount) {

        			  sum_sql+=Integer.parseInt(answers.getString(2));
        		  }
        		  else {
        			  break;
        		  }
        	  }
        	  else {
        		  break;
        	  }
        	  }

          } catch(SQLException e) {
              System.out.println("SQL#1 failed in cursor.");
              System.out.println(e.toString());
              System.exit(0);
          }

          // Close the cursor.
          try {
              answers.close();

              seedset.close();

          } catch(SQLException e) {
              System.out.print("SQL#1 failed closing cursor.\n");
              System.out.println(e.toString());
              System.exit(0);
          }
    	  return sum_sql>=amount;
      }

      private void insertloot() {
    	  String            queryText = "";     // The SQL text.
    	  String seedText = "";
          PreparedStatement querySt   = null;   // The query handle.
          ResultSet         answers   = null;   // A cursor.
          PreparedStatement seedstatement   = null;   // The query handle.
          ResultSet         seedset   = null;   // A cursor.

          queryText =
        		  "select * "+
        		  " from Treasure " +
        		  " order by random()";

          seedText = "select setseed (" + seed + ")";

          // Prepare the query.
          try {

        	  seedstatement = conDB.prepareStatement(seedText);

              querySt = conDB.prepareStatement(queryText);

          } catch(SQLException e) {
              System.out.println("SQL#1 failed in prepare");
              System.out.println(e.toString());
              System.exit(0);
          }

          // Execute the query.
          try {

        	  seedset= seedstatement.executeQuery();

              answers = querySt.executeQuery();
          } catch(SQLException e) {
              System.out.println("SQL#1 failed in execute");
              System.out.println(e.toString());
              System.exit(0);
          }
          //move the cursor
          try {

        	  if(seedset.next()) {

        	  }
        	  while(true) {
        	  if(answers.next()) {
        		  if(sum < amount) {
        			  sum+=Integer.parseInt(answers.getString(2));
        			  insertloot_with_treasure(answers.getString(1));
            		  counter++;
        		  }
        		  else {
        			  break;
        		  }
        	  }
        	  }

          } catch(SQLException e) {
              System.out.println("SQL#1 failed in cursor.");
              System.out.println(e.toString());
              System.exit(0);
          }




          // Close the cursor.
          try {
              answers.close();

              seedset.close();

          } catch(SQLException e) {
              System.out.print("SQL#1 failed closing cursor.\n");
              System.out.println(e.toString());
              System.exit(0);
          }
      }

      private void insertloot_with_treasure(String t) {
    	  PreparedStatement querySt   = null;   // The query handle.
          String queryText =
//        	"with T as (select treasure from loot where realm= "+realm+
//        	" and day="+date + " and theme=" + theme + ")"+

              " insert into loot" + "(loot_id, treasure, theme,realm,day,login) "
        		  +"values(?,?,?,?,?,null) ";
//               "where ?"+
//              " not exist in (select treasure from loot where theme=? and "+
//              " realm=? and "+ "day=?" +") as T";


              //theme realm day and success is null

              // Prepare the query.
              try {
                  querySt = conDB.prepareStatement(queryText);
              } catch(SQLException e) {
                  System.out.println("SQL#2 failed in prepare");
                  System.out.println(e.toString());
                  System.exit(0);
              }

              try {

                  querySt.setInt(1, counter);
                  querySt.setString(2, t);
                  querySt.setString(3, theme);
                  querySt.setString(4, realm);
                  querySt.setDate(5, date);

                  querySt.executeUpdate();
              } catch(SQLException e) {
                  System.out.println("SQL#1 failed in execute");
                  System.out.println(e.toString());
                  System.exit(0);
              }

              try {
                  querySt.close();
              } catch(SQLException e) {
                  System.out.print("SQL#1 failed closing the handle.\n");
                  System.out.println(e.toString());
                  System.exit(0);
              }




      }



        public static void main(String[] args) {
        CreateQuest ct = new CreateQuest(args);
    }
}
