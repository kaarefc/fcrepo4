package org.fcrepo.example.activemqlistener;

import org.apache.abdera.Abdera;
import org.apache.abdera.model.Entry;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.security.auth.login.LoginException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;

/**
 * Simply listens to active MQ and updates SOLR
 */
public class ActiveMQListener {
   	@Inject
   	private ActiveMQConnectionFactory connectionFactory;
    private Connection connection;
    private Session jmsSession;
    private MessageConsumer consumer;
    private Logger logger = LoggerFactory.getLogger(getClass());
    //FIXME: Hardcoded SOLR address
    SolrServer server = new HttpSolrServer( "http://localhost:8080/solr" );
    private final DocumentBuilder documentBuilder;

    public ActiveMQListener() throws Exception {
        documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    }

    @PostConstruct
   	public void acquireConnections() throws JMSException, LoginException {
   		logger.debug("Initializing: {}", this.getClass().getCanonicalName());

   		connection = connectionFactory.createConnection();
   		connection.start();
   		jmsSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		consumer = jmsSession.createConsumer(jmsSession.createTopic("fedora"));
        consumer.setMessageListener(new SOLRListener());
   	}

   	@PreDestroy
   	public void releaseConnections() throws JMSException {
   		logger.debug("Tearing down: {}", this.getClass().getCanonicalName());
   		consumer.close();
   		jmsSession.close();
   		connection.close();
   	}

    public SolrInputDocument solrDocumentFromFedoraId(String id) throws Exception {
        //TODO: Even more harcoded and hackish. Use decent REST framework instead of this abomination!
        String dc = new URL("http://localhost:8080/rest/" + id + "/datastreams/DC/").openConnection().getContent()
                .toString();
        return solrDocumentFromDC(id, dc);
    }

    private SolrInputDocument solrDocumentFromDC(String id, String dc) throws SAXException, IOException {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", id);
        Document document = documentBuilder.parse(dc);
        // Should actually understand the dc...
        NodeList dcNodes = document.getDocumentElement().getFirstChild().getChildNodes();
        for (int i = 0; i < dcNodes.getLength(); i++) {
            Node dcNode = dcNodes.item(i);
            doc.addField(dcNode.getNodeName(), dcNode.getTextContent());
        }
        return doc;
    }

    class SOLRListener implements MessageListener {
        @Override
        public void onMessage(Message message) {
            try {
                String text = ((TextMessage) message).getText();
                Entry entry = new Abdera().newEntry();
                entry.setContent(text);
                String id = entry.getId().toString();
                SolrInputDocument doc = solrDocumentFromFedoraId(id);
                if (doc != null) {
                    server.add(doc);
                }
            } catch (JMSException e) {
                logger.debug("Received message not understood: {}", message, e);
                // Ignore invalid message
            } catch (Exception e) {
                logger.debug("Received message not added to SOLR: {}", message, e);
                // Trouble adding solr document. Ignore
            }
        }
    }
}
