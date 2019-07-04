package com.marlonpatrick.alura.jms.v1_1;

import java.util.Hashtable;
import java.util.Scanner;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.naming.InitialContext;

public class ActiveMQQueueConsumerTest {
	
	public static void main(String[] args) throws Exception {
		
		Hashtable<String, String> jndiProperties = new Hashtable<String, String>();
		jndiProperties.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
		jndiProperties.put("java.naming.provider.url", "tcp://localhost:61616");		
		jndiProperties.put("queue.financeiro", "fila.financeiro");
		
		InitialContext context = new InitialContext(jndiProperties); 

		System.out.println("lookup(ConnectionFactory)...");		
        ConnectionFactory factory = (ConnectionFactory) context.lookup("ConnectionFactory");

		System.out.println("factory.createConnection...");
        Connection connection = factory.createConnection();

		System.out.println("connection.start...");
        connection.start();

		System.out.println("connection.createSession...");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		System.out.println("lookup(financeiro)...");
        Destination queue = (Destination) context.lookup("financeiro");

		System.out.println("session.createConsumer...");
        MessageConsumer consumer = session.createConsumer(queue);

        System.out.println("Waiting message...");
        consumer.setMessageListener(message -> System.out.println(message));
        
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        scanner.close();
        session.close();
        connection.close();
        context.close();
	}
}
