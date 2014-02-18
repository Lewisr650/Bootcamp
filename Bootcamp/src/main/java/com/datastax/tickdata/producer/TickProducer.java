//  This sends data to a rabbitmq queue topic, the topic is currently set to "TickData"
//  you must have rabbitmq installed which also requires erlang

package com.datastax.tickdata.producer;

import java.io.IOException;
import java.util.List;
import org.json.JSONException;

import com.cep.commons.EventObject;
import com.cep.darkstar.pubsub.pub.PublishTopic;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.tickdata.DataLoader;
import com.datastax.tickdata.producer.TickGenerator.TickValue;

public class TickProducer {

	public TickProducer() throws IOException, JSONException {
		long events = Long.parseLong(PropertyHelper.getProperty("noOfTicks", "1000000"));
		PublishTopic publish = new PublishTopic.Builder().topic("TickData").build();

		List<String> exchangeSymbols = DataLoader.getExchangeData();

		TickGenerator tickGenerator = new TickGenerator(exchangeSymbols);
		EventObject tick = new EventObject();
		TickValue tickValueRandom;
		
		for (long nEvents = 0; nEvents < events; nEvents++) {
			tickValueRandom = tickGenerator.getTickValueRandom();
			tick.put("Symbol",  tickValueRandom.symbol);
			tick.put("Price",  tickValueRandom.price);
			publish.publish(tick);
		}
		System.exit(0);
	}

	public static void main(String args[]) throws IOException, JSONException {
		new TickProducer();
	}
}