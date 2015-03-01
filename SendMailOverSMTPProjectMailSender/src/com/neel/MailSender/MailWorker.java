package com.neel.MailSender;

import java.io.IOException;

import com.neel.MQ.MQReciever;
import com.neel.MQ.MQSender;

public class MailWorker {

	private String workerId;
	private MQSender mqSender;
	private Thread heartbeat;
	private Thread workQueuePoller;

	public MailWorker(String id) throws Exception {

		workerId = id;
		mqSender = new MQSender();
		heartbeat = new Thread() {
			public void run() {
				while (true) {
					try {
						mqSender.sendMsg("WORKER_EXCHANGE", "HEARTBEAT",
								workerId);
						Thread.sleep(3600);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						System.exit(1);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		};
		try {
			// Register your self for work for the very first time

			mqSender.sendMsg("WORKER_EXCHANGE", "WORKER_QUEUE", workerId);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		// Notify its presence
		heartbeat.start();
	}

	public void pollMailQueue() {

		// Start polling the workId queue
		try {
			final MQReciever reciever = new MQReciever();
			reciever.createConsumer("MANAGER_EXCHANGE", workerId);
			workQueuePoller = new Thread() {
				public void run() {
					String message;
					while (true) {
						try {
							message = reciever.recvMessage().get(0);
							if ("START".equalsIgnoreCase(message)) {
								sendMails();
								break;
							} else if ("STOP".equalsIgnoreCase(message)) {
								System.out
										.println("Completed sending all emails. Now Exiting ...");
								heartbeat.interrupt();
								System.exit(0);
							}
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			};

			workQueuePoller.start();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void sendMails() {
		// TODO Auto-generated method stub
		ReadDBAndSendMails callSendMailsToAssignedIds = new ReadDBAndSendMails();
		try {
			callSendMailsToAssignedIds.sendMailsToAssignedIds(workerId);

			// After sending all emails ask for more work
			mqSender.sendMsg("WORKER_EXCHANGE", "WORKER_QUEUE", workerId);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			MailWorker worker = new MailWorker(args[0]);

			while (true) {
				worker.pollMailQueue();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
