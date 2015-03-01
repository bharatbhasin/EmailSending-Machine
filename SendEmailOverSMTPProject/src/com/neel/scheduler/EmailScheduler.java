package com.neel.scheduler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class EmailScheduler {

	private Connection connect = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;

	public void assignMailsToSender() throws Exception {
		try {
			// This will load the MySQL driver, each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");
			// Setup the connection with the DB
			connect = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/mydb?allowMultiQueries=false",
					"root", "root");

			// while (true) {
			preparedStatement = connect
					.prepareStatement("SELECT id from emailQueue LIMIT 5");
			resultSet = preparedStatement.executeQuery();

			// if (!resultSet.next())
			// break;
			// resultSet.beforeFirst();
			System.out.println("\nShowing selected Ids....\n");
			// AddMailInfoToDB callAddInfo=new AddMailInfoToDB();
			// callAddInfo.writeResultSet(resultSet);
			showSelectedIds(resultSet);
			// }

		} catch (Exception e) {
			throw e;
		} finally {
			close();
		}

	}

	public void showSelectedIds(ResultSet resultSet) throws SQLException {
		// ResultSet is initially before the first data set
		ArrayList<Integer> idList = new ArrayList<Integer>();
		while (resultSet.next()) {
			// It is possible to get the columns via name
			// also possible to get the columns via the column number
			// which starts at 1
			// e.g. resultSet.getSTring(2);

			int id = resultSet.getInt("id");
			idList.add(id);
			// String fromEmail = resultSet.getString("from_email_address");
			// String toEmail = resultSet.getString("to_email_address");
			// String subject = resultSet.getString("subject");
			// String body = resultSet.getString("body");
			// String status = resultSet.getString("status");
			// String senderId = resultSet.getString("senderId");
			System.out.println("id: " + id);
			// System.out.println("from: " + fromEmail);
			// System.out.println("to: " + toEmail);
			// System.out.println("subject: " + subject);
			// System.out.println("body: " + body);
			// System.out.println("status: " + status);
			// System.out.println("senderId: " + senderId);
		}

	}

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
