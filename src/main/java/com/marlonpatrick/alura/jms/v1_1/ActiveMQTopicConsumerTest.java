package com.marlonpatrick.alura.jms.v1_1;

import java.util.Hashtable;
import java.util.Scanner;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.InitialContext;

public class ActiveMQTopicConsumerTest {
	
	public static void main(String[] args) throws Exception {

		System.setProperty("org.apache.activemq.SERIALIZABLE_PACKAGES","*"); 

		Runnable stockCloseTask = createTopicSubscriber("estoque", "ebook is null or ebook=false");

		Runnable comercialCloseTask = createTopicSubscriber("comercial", null);

        System.out.println("Waiting message...");
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
        
        stockCloseTask.run();
        
        comercialCloseTask.run();
        
        scanner.close();
	}

	private static Runnable createTopicSubscriber(String id, String selector) throws Exception {
		Hashtable<String, String> jndiProperties = new Hashtable<String, String>();
		jndiProperties.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
		jndiProperties.put("java.naming.provider.url", "tcp://localhost:61616");		
		jndiProperties.put("topic.loja", "topico.loja");
		
		InitialContext context = new InitialContext(jndiProperties); 

		System.out.println("lookup(ConnectionFactory)...");		
        ConnectionFactory factory = (ConnectionFactory) context.lookup("ConnectionFactory");

		System.out.println("factory.createConnection...");
		Connection connection = factory.createConnection();
        connection.setClientID(id);

		System.out.println("connection.start...");
        connection.start();

		System.out.println("connection.createSession...");
		Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

		System.out.println("lookup(loja)...");
        Topic topic = (Topic) context.lookup("loja");

		System.out.println("session.createConsumer...");
        MessageConsumer consumer = session.createDurableSubscriber(topic, id, selector, false);

        consumer.setMessageListener(message -> {
        	System.out.println(id + ": " + message);
        	
        	if(message instanceof ObjectMessage) {
        		ObjectMessage objectMessage = (ObjectMessage)message;
            	try {
					System.out.println(objectMessage.getObject());
					session.commit();
				} catch (JMSException e) {
					try {
						session.rollback();
					} catch (JMSException e1) {
						throw new RuntimeException(e1);
					}
					throw new RuntimeException(e);
				}
        	}
        });
        
        return new Runnable() {
			
			@Override
			public void run() {
		        try {
					session.close();
			        connection.close();
			        context.close();				
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
	}
}
