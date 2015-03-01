package com.neel.scheduler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class AddMailInfoToDB {

	private Connection connect = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;

	public void AddEmailsToDBTAble() throws Exception {
		try {
			// This will load the MySQL driver, each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");
			// Setup the connection with the DB
			connect = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/mydb?allowMultiQueries=false",
					"root", "root");

			// Statements allow to issue SQL queries to the database
			// statement = connect.createStatement();
			// Result set get the result of the SQL query
			/*
			 * resultSet = statement .executeQuery("select * from emailQueue");
			 * writeResultSet(resultSet);
			 */
			// delete before inserting to avoid conflict...

			/*
			 * preparedStatement = connect
			 * .prepareStatement("delete from emailInfo where id= ? ; ");
			 * preparedStatement.setInt(1, 2);
			 * preparedStatement.executeUpdate();
			 * 
			 * resultSet = statement .executeQuery("select * from emailInfo");
			 */

			// ***********************************************************************************************
			// Getting total no of entries.........................
			preparedStatement = connect
					.prepareStatement("SELECT count(id) from emailQueue");
			resultSet = preparedStatement.executeQuery();

			int count = 0;
			while (resultSet.next()) {
				count += resultSet.getInt(1);
			}
			System.out.println("count=" + count);

			// *************************************************************************************************
			Scanner in = new Scanner(System.in);
			// PreparedStatements can use variables and are more efficient
			preparedStatement = connect
					.prepareStatement("insert into  emailQueue values ( ?, ?, ?, ? ,?,?,?)");
			// "id, from_email_address, to_email_address, subject, body from emailInfo");
			// Parameters start with 1
			count++;
			preparedStatement.setInt(1, count);
			preparedStatement.setString(2, "neelendrakmr3@gmail.com");
			System.out.println("enter to_email_address....\n");
			String to_email = in.nextLine();
			preparedStatement.setString(3, to_email);

			System.out.println("enter subject of the mail....\n");
			String subject = in.nextLine();

			preparedStatement.setString(4, subject);

			System.out.println("enter body of the mail....\n");
			String body = in.nextLine();
			preparedStatement.setString(5, body);

			preparedStatement.setString(6, "pending");
			preparedStatement.setString(7, "");

			preparedStatement.executeUpdate();
			System.out.println("\n Mail entry added to table......\n"
					+ "All table entries are....\n");
			preparedStatement = connect
					.prepareStatement("SELECT * from emailQueue");
			resultSet = preparedStatement.executeQuery();
			writeResultSet(resultSet);
			in.close();
		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	/*
	 * public void getEmailInfoAndSendMail() throws Exception {
	 * 
	 * 
	 * 
	 * try { // This will load the MySQL driver, each DB has its own driver
	 * Class.forName("com.mysql.jdbc.Driver"); // Setup the connection with the
	 * DB connect = DriverManager
	 * .getConnection("jdbc:mysql://localhost:3306/mydb?allowMultiQueries=false"
	 * ,"root","");
	 * 
	 * // Statements allow to issue SQL queries to the database
	 * 
	 * preparedStatement = connect .prepareStatement(
	 * "SELECT id, from_email_address, to_email_address, subject, body from emailInfo"
	 * ); resultSet = preparedStatement.executeQuery();
	 * writeResultSet(resultSet);
	 * 
	 * } catch (Exception e) { throw e; } finally { close(); }
	 * 
	 * 
	 * 
	 * 
	 * 
	 * }
	 */
	public void writeResultSet(ResultSet resultSet) throws SQLException {
		// ResultSet is initially before the first data set
		while (resultSet.next()) {
			// It is possible to get the columns via name
			// also possible to get the columns via the column number
			// which starts at 1
			// e.g. resultSet.getSTring(2);

			int id = resultSet.getInt("id");
			String fromEmail = resultSet.getString("from_email_address");
			String toEmail = resultSet.getString("to_email_address");
			String subject = resultSet.getString("subject");
			String body = resultSet.getString("body");
			String status = resultSet.getString("status");
			String senderId = resultSet.getString("senderId");
			System.out.println("id: " + id);
			System.out.println("from: " + fromEmail);
			System.out.println("to: " + toEmail);
			System.out.println("subject: " + subject);
			System.out.println("body: " + body);
			System.out.println("status: " + status);
			System.out.println("senderId: " + senderId);
		}
	}

	// You need to close the resultSet
	private void close() {
		try {
			if (resultSet != null) {
				resultSet.close();
			}

			if (statement != null) {
				statement.close();
			}

			if (connect != null) {
				connect.close();
			}
		} catch (Exception e) {

		}
	}

}
