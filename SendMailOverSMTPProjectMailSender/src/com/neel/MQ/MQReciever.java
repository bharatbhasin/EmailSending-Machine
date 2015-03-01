package com.neel.MQ;

import java.io.IOException;
import java.util.ArrayList;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

public class MQReciever {

	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	private QueueingConsumer consumer;
	private String queueName;

	public MQReciever() throws Exception {
		factory = new ConnectionFactory();
		factory.setHost("localhost");
		connection = factory.newConnection();
		channel = connection.createChannel();
	}

	public void createConsumer(String exchange_name, String binding_key,
			String queueName) throws IOException {
		channel.exchangeDeclare(exchange_name, "direct");
		channel.queueDeclare(queueName, false, true, true, null);
		channel.queueBind(queueName, exchange_name, binding_key);
		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume(queueName, true, consumer);
		this.queueName = queueName;
	}

	public void createConsumer(String exchange_name, String binding_key)
			throws IOException {
		channel.exchangeDeclare(exchange_name, "direct");
		String queueName = channel.queueDeclare().getQueue();
		channel.queueBind(queueName, exchange_name, binding_key);
		consumer = new QueueingConsumer(channel);
		channel.basicConsume(queueName, true, consumer);
		this.queueName = queueName;
	}

	public ArrayList<String> recvMessage() throws Exception {

		System.out.println(consumer);
		ArrayList<String> messages = new ArrayList<String>();
		for (int i = 0; i < channel.queueDeclarePassive(queueName)
				.getMessageCount(); i++) {
			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			String message = new String(delivery.getBody());
			messages.add(message);
			String routingKey = delivery.getEnvelope().getRoutingKey();
		}
		channel.queuePurge(queueName);
		return messages;
	}

	public static void main(String[] argv) throws Exception {

		MQReciever rec1 = new MQReciever();
		rec1.createConsumer("WORKER_EXCHANGE", "WORKER_QUEUE");
		while (true) {
			System.out
					.println(" [*] Waiting for messages. To exit press CTRL+C");
			System.out.println(rec1.recvMessage().get(0));
		}
	}
}