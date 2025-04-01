import com.mysql.cj.jdbc.MysqlDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/* Simple application that simulates a storefront which needs to communicate with a MySQL database.
* The database has 2 tables: Order (parent) and Order details (child).
* This program demonstrates ability to write and execute CRUD operations in a transaction.
*
* Alongside an IDE, MySQL workbench was used to set up the initial table and verify operation results.
* */
public class Main
{
	public static void main(String[] args)
	{
		//server is run on local machine
		var dataSource = new MysqlDataSource();
		dataSource.setServerName("localhost");
		dataSource.setURL("jdbc:mysql://localhost:3306/storefront?continueBatchOnError=false");
		dataSource.setPort(3306);
		dataSource.setUser(System.getenv("MYSQLUSER"));
		dataSource.setPassword(System.getenv("MYSQLPASS"));

		try(Connection conn = dataSource.getConnection();
			Statement statement = conn.createStatement())
		{
			String tableName = "storefront.order";
			String columnName = "order_date";
			String columnValue = "2025-01-28 01:01:01"; //example date

			//if the order and details don't exist, create them
			//otherwise, delete them
			if(!executeSelect(statement, tableName, columnName, columnValue))
			{
				System.out.println("record does not exist");
				insertOrderWithDetails(conn, statement, columnValue);
				executeSelect(statement, "storefront.order", "order_date", columnValue);
			}
			else
			{
				System.out.println("Order exists, attempting delete");
				deleteOrderWithDetails(conn, statement, columnValue);
				System.out.println("Order deleted");
			}
		}
		catch(SQLException e)
		{
			throw new RuntimeException(e);
		}
	}

	//method to delete an order, identifying it by the date passed via string
	private static void deleteOrderWithDetails(Connection conn, Statement statement,
										  String orderDate) throws SQLException
	{
		try
		{
			conn.setAutoCommit(false);

			//cascade delete is used for this table
			//deleting the parent will delete children as well
			String deleteOrder = "DELETE FROM storefront.order WHERE order_date='%s'".formatted(orderDate);
			int results = statement.executeUpdate(deleteOrder);

			System.out.println("row count for DML: " + results); //print some confirmation data

			conn.commit();
		}
		catch (SQLException e)
		{
			//rollback any changes if there was a problem during the delete
			e.printStackTrace();
			conn.rollback();
		}
		conn.setAutoCommit(true);
	}

	//make a new order, with supporting (placeholder) details
	//for simplicity's sake, the details are in this method
	//however, they could be passed via method parameter
	//or added at a later time
	private static void insertOrderWithDetails(Connection conn, Statement statement,
										  String orderDate) throws SQLException
	{
		try
		{
			conn.setAutoCommit(false); //set to false as best practice

			//MySQL statement that will insert parent order
			String orderInsert = "INSERT INTO storefront.order (order_date) VALUES (%s)"
					.formatted(statement.enquoteLiteral(orderDate));

			System.out.println(orderInsert); //print the statement being executed
			statement.execute(orderInsert, Statement.RETURN_GENERATED_KEYS); //tells the DB we need keys back
			ResultSet rs = statement.getGeneratedKeys(); //get the new auto-generated keys for child

			//if the data we got back from auto-generated keys wasn't null and has a next value,
			//get the value in the first column from result set, otherwise set to -1
			int orderId = (rs != null && rs.next()) ? rs.getInt(1) : -1;

			//hardcoding placeholder descriptions as an example, but this could be gotten from somewhere else
			String[] itemDescriptions = new String[]
					{
							"description1", "description2"
					};

			//insertion MySQL string, used inside the for loop below
			String detailInsert = "INSERT INTO storefront.order_details (item_description, order_id) VALUES (%s, %d)";

			//putting this in a loop if there are more than 2 descriptions
			for(int i = 0; i < itemDescriptions.length; i++)
			{
				//using the enquoteLiteral() helper method to properly quote for us
				String detailQuery = detailInsert.formatted(statement.enquoteLiteral(itemDescriptions[i]), orderId);
				System.out.println(detailQuery);
				statement.execute(detailQuery);
			}
			conn.commit();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			conn.rollback(); //rollback any changes if there are problems
		}
		conn.setAutoCommit(true);
	}

	//method mostly used to return information about the table, like verifying CRUD operations
	//it will execute a SELECT query on the given table, column name and column value
	private static boolean executeSelect(Statement statement, String table,
										 String columName, String columnValue) throws SQLException
	{
		String query = "SELECT * FROM %s WHERE %s='%s'".formatted(table, columName, columnValue);
		System.out.println(query);
		var rs = statement.executeQuery(query);
		if(rs != null)
		{
			return printRecords(rs);
		}
		return false;
	}

	//another utility method that will print information to the console
	//specifically, given a ResultSet it will print the data within in a nicely formatted way
	private static boolean printRecords(ResultSet resultSet) throws SQLException
	{
		boolean foundData = false;
		var meta = resultSet.getMetaData();
		for(int i = 1; i <= meta.getColumnCount(); i++)
		{
			System.out.printf("%-15s", meta.getColumnName(i).toUpperCase());
		}
		System.out.println();
		while(resultSet.next())
		{
			for(int i = 1; i <= meta.getColumnCount(); i++)
			{
				System.out.printf("%-15s", resultSet.getString(i));
			}
			System.out.println();
			foundData = true;
		}
		return foundData;
	}
}
