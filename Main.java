import com.mysql.cj.jdbc.MysqlDataSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/* Simple application that simulates a storefront which needs to communicate with a MySQL database.
 * The database has 2 tables: Order (parent) and Order details (child).
 * In this particular program, DDL statements were written and executed to create a new schema.
 * This is in addition to using CRUD operations in a transaction, with PreparedStatements.
 *
 * As an additional challenge, some of the input orders had their dates purposefully formatted incorrectly.
 * The code needed extra design to parse and decide if dates were properly formatted.
 * If they weren't, the data following it was ignored.
 *
 * Alongside an IDE, MySQL workbench was used to set up the initial table and verify operation results.
 * Sample orders that were used for testing were stored and accessed in a .csv file
 * */

public class Main
{
	private static final int MYSQL_DB_NOT_FOUND = 1049; //error code for MySQL

	//MySQL statements that will be used for future PreparedStatements
	private static String USE_SCHEMA = "USE storefront";
	private static String ORDER_INSERT =
			"INSERT INTO storefront.order (order_date) VALUES(?)";
	private static String ORDER_DETAIL_INSERT =
			"INSERT INTO storefront.order_details (quantity, item_description, order_id) VALUES (?, ?, ?)";

	public static void main(String[] args)
	{
		var dataSource = new MysqlDataSource();
		dataSource.setServerName("localhost");
		dataSource.setPort(3306);
		dataSource.setUser(System.getenv("MYSQLUSER"));
		dataSource.setPassword(System.getenv("MYSQLPASS"));

		try(Connection conn = dataSource.getConnection())
		{
			DatabaseMetaData metaData = conn.getMetaData(); //getting this so we can read any error codes
			System.out.println("getSQLStateType: " + metaData.getSQLStateType()); //drivers/vendors will have different codes
			System.out.println("--------------------------------------");

			//if the schema doesn't exist, make it and exit the program
			if(!checkSchema(conn))
			{
				System.out.println("storefront schema does not exist");
				setUpSchema(conn);
				System.out.println("storefront created, exiting program");
				System.out.println("re-run to begin read operations");
				return;
			}

			//primary method that handles reading data from the csv
			addDataFromFile(conn);
		}
		catch(SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	//primary method that handles reading data from the csv
	private static void addDataFromFile(Connection conn) throws SQLException
	{
		List<String> records = null; //this field will hold data read from the file
		try
		{
			records = Files.readAllLines(Path.of("src\\Orders.csv"));
		}
		catch(IOException ioe)
		{
			throw new RuntimeException(ioe);
		}

		/*
		* The CSV file has data formatted as such (... indicate some number of repeating items) :
		*
		* <date>
		* <item, quantity, description>
		* <item, quantity, description>
		* ...
		* <item, quantity, description>
		* <date>
		* <item, quantity, description>
		* ...
		*
		* because of this, need to be checking at the start of each line if a new date is detected
		* if new date is found, start a new order
		* */

		String lastDate = null;

		//using try with resources, need the generated keys for children
		try(PreparedStatement psOrder = conn.prepareStatement(ORDER_INSERT, Statement.RETURN_GENERATED_KEYS);
			PreparedStatement psOrderDetail = conn.prepareStatement(ORDER_DETAIL_INSERT, Statement.RETURN_GENERATED_KEYS)
		)
		{
			//there are some deliberately bad dates in the csv
			//need to batch orders by date, if the date is invalid or something happens
			//need to be able to continue and only rollback the bad order
			int currentOrderId = -1;
			conn.setAutoCommit(false);
			boolean badDate = false;
			List<Integer> totalInserts = new ArrayList<>(); //for future validation and printing
			for(String record : records)
			{
				String[] columns = record.split(",");
				System.out.println(record);

				//badDate should be false most of the time, printing it for debug
				System.out.println("currentOrderId: " + currentOrderId + " - badDate: " + badDate);
				System.out.print("columned: "); //formatting the print so it's easier to read
				for(String s : columns)
				{
					//printing the parsed line from the csv, but haven't done anything yet
					System.out.print(s + " - ");
				}
				System.out.print("\n");
				//check to see if the first string says "order" or "item"

				//if "order", get the date from the second column
				if(columns[0].equalsIgnoreCase("order"))
				{
					System.out.println("new order");
					if(lastDate != null) //if there is a previous date, means there is stuff in the batch
						//so need to be cleared by pushing changes to the DB or abandoning the parsed data
					{
						//if the date given was invalid or something else is wrong
						if(badDate)
						{
							//don't forget to clear flags!
							System.out.println("bad date caught: " + lastDate);
							lastDate = null;
							badDate = false;
							psOrderDetail.clearBatch();
							conn.rollback();
						}
						else
						//the date was valid, can commit changes to the DB
						{
							System.out.println("attempting to execute changes");
							//add the update count to master List and commit changes
							int[] inserts = psOrderDetail.executeBatch();
							//adding stuff to the totalInserts for validation
							totalInserts.addAll(Arrays.stream(inserts)
									.boxed()
									.toList());
							conn.commit();
							//print debug information
							System.out.println("inserts: " + inserts.length);
							System.out.println("totalInserts: " + totalInserts.size());
						}
					}

					//at this point of reading the CSV it is still "looking at" the new date to parse
					try
					{
						lastDate = columns[1]; //set lastDate to this line's date

						//call insertOrder, which inserts an order to the Order table
						//returns the auto-gen'd key which is needed and stores in currentOrderId
						currentOrderId = insertOrder(psOrder, columns[1]);
						System.out.println("new order date: " + lastDate);
						System.out.println("new order id: " + currentOrderId);
					}
					catch(SQLException e2)
					{
						//if there's a problem, probably means a bad date was passed
						//set last date to null or something
						System.out.println("error parsing lastDate (probably a bad date): " + lastDate);
						System.out.println("error text: " + e2);
						badDate = true;
					}
				}
				else if(columns[0].equalsIgnoreCase("item") && lastDate != null && currentOrderId > 0)
				{
					System.out.println("trying to add a new item");
					//add an item
					//using temp vars to improve readability
					int newQuantity = Integer.parseInt(columns[1]);
					String newDescription = columns[2];

					//inserts new order details using the data parsed from the CSV
					//code cannot reach here if the lastDate or currentOrderId is bad
					insertOrderDetails(psOrderDetail, newQuantity, newDescription, currentOrderId);
				}
				else
				{
					//most typically this block is only reached when the date is incorrectly formatted
					System.out.println("bad date or something in addDataFromFile()");
					System.out.println("record: " + record);
					System.out.println("lastDate: " + lastDate);
				}

			}
			//only insert on successful check
			//this is the second time this block is called since there will be no "end of file" markers in the csv
			//q.e.d. there will be order details in the batch that need to be committed or abandoned
			if(!badDate)
			{
				//only call executeBatch on orderdetails
				//add the update count to our master List and commit changes
				int[] inserts = psOrderDetail.executeBatch();
				totalInserts.addAll(Arrays.stream(inserts)
						.boxed()
						.toList());
				conn.commit();
			}

			//executeUpdate already called for orderId in insertOrder();
			System.out.printf("%d records added%n", totalInserts.size());
			conn.commit();
			conn.setAutoCommit(true);
		}
		catch(SQLException e)
		{
			conn.rollback();
			throw new RuntimeException(e);
		}
	}

	//inserts a new order given date as a string, does not include orderDetails
	//returns the autogenerated int, or -1 as a default if nothing was inserted
	private static int insertOrder(PreparedStatement ps,
										  String orderDate) throws SQLException
	{
		int newOrderId = -1;
		ps.setString(1, orderDate);
		int insertedCount = ps.executeUpdate();
		if(insertedCount > 0) //if there was a successful insertion
		{
			ResultSet generatedKeys = ps.getGeneratedKeys();
			if(generatedKeys.next())
			{
				newOrderId = generatedKeys.getInt(1);
				System.out.println("Auto-incremented ID for order insert: " + newOrderId);
			}
		}
		return newOrderId;
	}

	//inserts order details, requires knowing what order (and its associated ID) beforehand
	//adds one orderDetail per call, so as an example, 5 orderDetail inserts need 5 calls to this method
	//this method doesn't commit changes, it adds to the batch
	private static void insertOrderDetails(PreparedStatement ps, int inputQuantity, String description,
										  int orderId) throws SQLException
	{
		ps.setInt(1, inputQuantity);
		ps.setString(2, description);
		ps.setInt(3, orderId);
		ps.addBatch();
	}

	//initializes the DB with cascade delete, so deleting a parent will delete children
	private static void setUpSchema(Connection conn) throws SQLException
	{
		//strings to initialize the storefront schema, order table
		String createSchema = "CREATE SCHEMA storefront";
		String createOrder = """
    			CREATE TABLE storefront.order(
    			order_id int NOT NULL AUTO_INCREMENT,
    			order_date DATETIME NOT NULL,
    			PRIMARY KEY (order_id)
				)""";

		//sets up order_detail table with parent-child relationship between the 2 tables (cascade deletion)
		//when the parent is deleted, they are treated as a single unit
		//in plain english: when an order is deleted, details related to that order are also deleted
		String createOrderDetails = """
   				CREATE TABLE storefront.order_details (
   				order_detail_id int NOT NULL AUTO_INCREMENT,
   				quantity int NOT NULL,
   				item_description text,
   				order_id int DEFAULT NULL,
   				PRIMARY KEY (order_detail_id),
   				KEY FK_ORDERID (order_id),
   				CONSTRAINT FK_ORDERID FOREIGN KEY (order_id)
   				REFERENCES storefront.order (order_id) ON DELETE CASCADE
   				) """;

		//DDL operations to create the schema and tables as written in the above strings
		try(Statement statement = conn.createStatement())
		{
			//DDL operations don't typically use PreparedStatement
			//usually only for DML operations, where statements are executed multiple times
			System.out.println("Creating storefront Database");
			statement.execute(createSchema);
			if(checkSchema(conn))
			{
				statement.execute(createOrder);
				System.out.println("Successfully Created Order");
				statement.execute(createOrderDetails);
				System.out.println("Successfully Created Order Details");
			}
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
	}

	//helper method to check if the schema already exists
	//returns true by default, unless there's an error thrown, the vendor is MySQL and
	//the error code matches MYSQL_DB_NOT_FOUND (1049), since error codes can vary from vendor to vendor
	private static boolean checkSchema(Connection conn) throws SQLException
	{
		try(Statement statement = conn.createStatement())
		{
			statement.execute(USE_SCHEMA);
		}
		catch(SQLException e)
		{
			e.printStackTrace();
			System.err.println("SQLState: " + e.getSQLState());
			System.err.println("Error Code: " + e.getErrorCode());
			System.err.println("Message: " + e.getMessage());

			if(conn.getMetaData().getDatabaseProductName().equals("MySQL")
					&& e.getErrorCode() == MYSQL_DB_NOT_FOUND)
			{
				return false;
			}
			else { throw e; }
		}
		return true;
	}
}
