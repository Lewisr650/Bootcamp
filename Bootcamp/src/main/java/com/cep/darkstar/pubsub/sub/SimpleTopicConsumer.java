package com.cep.darkstar.pubsub.sub;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class SimpleTopicConsumer {
	static Logger logger = Logger.getLogger("com.cep.darkstar.offramp.SimpleTopicConsumer");
    public static void main(String[] args) {
        try {
            if (args.length < 1 || args.length > 5) {
                System.err.print("Usage: SimpleTopicConsumer brokerhostname [topic\n" +
                                 "                                          [host\n" +
                                 "                                          [port\n" +
                                 "where\n" +
                                 " - topic defaults to \"#\",\n" +
                                 " - host to \"localhost\", and\n" +
                                 " - port to 5672\n");
                System.exit(1);
            }
    		// initialize log4j
    		PropertyConfigurator.configure("log4j.properties");
    		
        	long end = 0;
        	long begin = 0;
        	int i = 0;
        	String topicPattern = (args.length > 0) ? args[0] : "#";
            String hostName = (args.length > 1) ? args[1] : "localhost";
            int portNumber = (args.length > 2) ? Integer.parseInt(args[1]) : 5672;

            SubscribeTopic listen = new SubscribeTopic.Builder().hostName(hostName).portNumber(portNumber).topic(topicPattern).build();
			begin = System.currentTimeMillis();
            while (true) {
            	i =  i + 1;
            	System.out.println(new String(listen.nextDelivery()));
                if ((i % 50000) == 0) {
    				end = System.currentTimeMillis();
    				logger.info("50,000 messages sent in "+Long.toString((end-begin)/1000)+" seconds");
    				begin = end;
    			}
            }
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            System.exit(1);
        }
    }
}
