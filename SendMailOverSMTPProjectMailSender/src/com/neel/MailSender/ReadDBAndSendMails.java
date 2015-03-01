package com.neel.MailSender;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import com.neel.MailSender.AssignToThreads;

public class ReadDBAndSendMails {

	private Connection connect = null;
	private Statement statement = null;
	private PreparedStatement preparedStatement = null;
	private ResultSet resultSet = null;

	public void sendMailsToAssignedIds(String workerId) throws Exception {
		try {
			// This will load the MySQL driver, each DB has its own driver

			Class.forName("com.mysql.jdbc.Driver");
			// Setup the connection with the DB
			connect = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/mydb?allowMultiQueries=false",
					"root", "root");

			preparedStatement = connect
					.prepareStatement("SELECT * from emailQueue where senderId='"
							+ workerId + "'");
			resultSet = preparedStatement.executeQuery();

			// ArrayList AssignedIdList=new ArrayList();
			// while (resultSet.next()) {
			// int id = resultSet.getInt("id");
			// AssignedIdList.add(id);
			// System.out.println("id: " + id);
			// }

			// System.out.println(AssignedIdList.get(0));
			AssignToThreads callSendMailThreads = new AssignToThreads();
			callSendMailThreads.SendMailThreads(resultSet);

		} catch (Exception e) {
			throw e;
		} finally {
			close();
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
