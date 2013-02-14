package org.fcrepo.example.activemqlistener;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.security.auth.login.LoginException;

/**
 * Simply listens to active MQ and prints all actions to stdout
 */
public class ActiveMQListener {
   	@Inject
   	private ActiveMQConnectionFactory connectionFactory;
    private Connection connection;
    private Session jmsSession;
    private MessageConsumer consumer;
    private Logger logger = LoggerFactory.getLogger(getClass());

    @PostConstruct
   	public void acquireConnections() throws JMSException, LoginException {
   		logger.debug("Initializing: {}", this.getClass().getCanonicalName());

   		connection = connectionFactory.createConnection();
   		connection.start();
   		jmsSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		consumer = jmsSession.createConsumer(jmsSession.createTopic("fedora"));
        consumer.setMessageListener(new SystemOutListener());
   	}

   	@PreDestroy
   	public void releaseConnections() throws JMSException {
   		logger.debug("Tearing down: {}", this.getClass().getCanonicalName());
   		consumer.close();
   		jmsSession.close();
   		connection.close();
   	}

    private class SystemOutListener implements MessageListener {
        @Override
        public void onMessage(Message message) {
            System.out.println(message);
        }
    }
}
