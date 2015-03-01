package com.neel.MQ;

import java.io.IOException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

public class MQSender {

	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;

	public MQSender() throws Exception {
		factory = new ConnectionFactory();
		factory.setHost("localhost");
		connection = factory.newConnection();
		channel = connection.createChannel();
	}

	public void sendMsg(String exchangeName, String routingKey, String message)
			throws IOException {
		channel.exchangeDeclare(exchangeName, "direct");
		channel.basicPublish(exchangeName, routingKey, null, message.getBytes());
		System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
		channel.close();
		connection.close();
	}

	public static void main(String[] argv) throws Exception {

		MQSender sender = new MQSender();
		sender.sendMsg("WORKER_EXCHANGE", "WORKER_QUEUE", "HELLO");
	}

}