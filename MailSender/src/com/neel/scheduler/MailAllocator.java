package com.neel.scheduler;

import java.util.ArrayList;
import java.util.HashMap;

import com.neel.MQ.MQReciever;
import com.neel.MQ.MQSender;

//import com.neel.scheduler.AddMailInfoToDB;

public class MailAllocator {

	private HashMap<String, String> availableWorkers = new HashMap<String, String>();
	private HashMap<String, String> aliveWorkers = new HashMap<String, String>();
	private Thread workerManager;
	private Thread heartbeatChecker;
	private long unsentMails;

	public void findAvailableWorkers() {

		workerManager = new Thread() {
			public void run() {
				try {
					MQReciever workerQueueRec = new MQReciever();
					while (unsentMails > 0) {
						ArrayList<String> availableWorkerList;
						workerQueueRec.createConsumer("WORKER_EXCHANGE",
								"WORKER_QUEUE", "MANAGER_QUEUE");
						availableWorkerList = workerQueueRec.recvMessage();
						for (String workerId : availableWorkerList) {
							availableWorkers.put(workerId, "AVAILABLE");
							aliveWorkers.put(workerId, "ALIVE");
						}
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		workerManager.start();
	}

	public void checkAliveWorkers() {

		heartbeatChecker = new Thread() {
			public void run() {
				try {
					MQReciever workerQueueRec = new MQReciever();
					while (unsentMails > 0) {
						ArrayList<String> aliveWorkerList;
						workerQueueRec.createConsumer("WORKER_EXCHANGE",
								"HEARTBEAT", "HEARTBEAT_QUEUE");
						aliveWorkerList = workerQueueRec.recvMessage();
						// Before checking for alive workers clear this map to
						// fill it again
						aliveWorkers.clear();
						for (String workerId : aliveWorkerList) {
							aliveWorkers.put(workerId, "ALIVE");
						}
						Thread.sleep(3600);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};
		heartbeatChecker.start();
	}

	public void assignEmailsToSend() throws Exception {

		// Send START to all available workers
		MQSender msgSender = new MQSender();
		while (unsentMails > 0) {
			if (availableWorkers.isEmpty()) {
				findAvailableWorkers();
			} else if (!aliveWorkers.isEmpty()) {
				// Change status in DB and send START signal to workers
				for (String workerId : availableWorkers.keySet()) {
					if (aliveWorkers.containsKey(workerId)) {
						// Change the status of the emails to allocated and
						// assign
						// the
						// worker ids
						msgSender
								.sendMsg("MANAGER_EXCHANGE", workerId, "START");
						availableWorkers.remove(workerId);
					} else {
						// Change the status of all database entries for this
						// worker id to pending again
					}
				}
			}
		}
	}

	private void findUnsentMails() {
		// Calculate the count of unsent emails
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		// AddMailInfoToDB callAddInfo = new AddMailInfoToDB();
		// callAddInfo.AddEmailsToDBTAble();
		//
		/*
		 * EmailScheduler callScheduleMails = new EmailScheduler();
		 * callScheduleMails.assignMailsToSender();
		 */

		MailAllocator allocator = new MailAllocator();
		allocator.assignEmailsToSend();
	}
}
