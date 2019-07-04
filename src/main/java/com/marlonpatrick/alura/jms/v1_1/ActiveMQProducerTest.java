package com.marlonpatrick.alura.jms.v1_1;

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.InitialContext;

import br.com.caelum.modelo.Pedido;
import br.com.caelum.modelo.PedidoFactory;

public class ActiveMQProducerTest {
	
	public static void main(String[] args) throws Exception {
		
		Hashtable<String, String> jndiProperties = new Hashtable<String, String>();
		jndiProperties.put("java.naming.factory.initial", "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
		jndiProperties.put("java.naming.provider.url", "tcp://localhost:61616");		
		jndiProperties.put("queue.financeiro", "fila.financeiro");
		jndiProperties.put("topic.loja", "topico.loja");

		String destinationName = "loja";
		
		InitialContext context = new InitialContext(jndiProperties); 

		System.out.println("lookup(ConnectionFactory)...");		
        ConnectionFactory factory = (ConnectionFactory) context.lookup("ConnectionFactory");

		System.out.println("factory.createConnection...");
        Connection connection = factory.createConnection();

		System.out.println("connection.start...");
        connection.start();

		System.out.println("connection.createSession...");
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		System.out.println("lookup(destination)...");
        Destination destination = (Destination) context.lookup(destinationName);

		System.out.println("session.createProducer...");
        MessageProducer producer = session.createProducer(destination);

		System.out.println("producer.send...");
//        for (int i = 1; i <= 5; i++) {
//        	TextMessage message = session.createTextMessage("Message " + i);
//        	message.setBooleanProperty("ebook", (i % 2)==0);
//        	producer.send(message);			
//		}

		Pedido pedido = new PedidoFactory().geraPedidoComValores();
		Message message = session.createObjectMessage(pedido);
    	producer.send(message);			
		
		System.out.println("close...");
        session.close();
        connection.close();
        context.close();
	}
}
