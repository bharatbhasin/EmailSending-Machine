package com.neel.MailSender;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AssignToThreads implements Runnable {

	private int id;
	private String from;
	private String to;
	private String subject;
	private String body;

	public AssignToThreads() {

	}

	public AssignToThreads(int id, String from, String to, String subject,
			String body) {
		this.id = id;
		this.from = from;
		this.to = to;
		this.subject = subject;
		this.body = body;
	}

	// @Override
	public void run() {
		System.out.println(Thread.currentThread().getName() + " Start. id = "
				+ id);
		System.out.println(Thread.currentThread().getName()
				+ " Start. from_email_address = " + from);
		System.out.println(Thread.currentThread().getName()
				+ " Start. to_email_address = " + to);
		System.out.println(Thread.currentThread().getName()
				+ " Start. subject = " + subject);
		System.out.println(Thread.currentThread().getName() + " Start. body = "
				+ body);
		// processCommand();

		System.out.println(Thread.currentThread().getName() + " End.");
	}

	public void processCommand() {
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();

		}
	}

	// @override
	// public String toString()
	// {
	// return this.command;
	// }

	public void SendMailThreads(ResultSet resultSet) throws SQLException {
		ExecutorService executor = Executors.newFixedThreadPool(5);
		/*
		 * for (int i = 0; i < AssignedIdList.size(); i++) {
		 * 
		 * Runnable worker = new AssignToThreads(" " + AssignedIdList.get(i));
		 * executor.execute(worker);
		 * 
		 * }
		 */
		while (resultSet.next()) {
			// It is possible to get the columns via name
			// also possible to get the columns via the column number
			// which starts at 1
			// e.g. resultSet.getSTring(2);

			// int id = resultSet.getInt("id");
			// idList.add(id);
			// String fromEmail = resultSet.getString("from_email_address");
			// String toEmail = resultSet.getString("to_email_address");
			// String subject = resultSet.getString("subject");
			// String body = resultSet.getString("body");
			// String status = resultSet.getString("status");
			// String senderId = resultSet.getString("senderId");
			// System.out.println("id: " + id);
			// System.out.println("from: " + fromEmail);
			// System.out.println("to: " + toEmail);
			// System.out.println("subject: " + subject);
			// System.out.println("body: " + body);
			// System.out.println("status: " + status);
			// System.out.println("senderId: " + senderId);

			Runnable worker = new AssignToThreads(resultSet.getInt("id"),
					resultSet.getString("from_email_address"),
					resultSet.getString("to_email_address"),
					resultSet.getString("subject"), resultSet.getString("body"));

			executor.execute(worker);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {

		}
		System.out.println("finished All Threads");
	}

}
