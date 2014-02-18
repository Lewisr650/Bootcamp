package com.datastax.tickdata.consumer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;

import org.mortbay.log.Log;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.tickdata.model.TickData;

public class TickConsumer {

	private BlockingQueue<List<TickData>> queueTickData;
	private final ConsumerConnector consumer;
	private String topic;
	private long TOTAL_COUNT = 0;

	public TickConsumer(BlockingQueue<List<TickData>> queueTickData) {
		this.queueTickData = queueTickData;

		ConsumerConfig consumerConfig = createConsumerConfig(PropertyHelper.getProperty("zk", "localhost:2181"),
				PropertyHelper.getProperty("consumerGroup", "test-group"));
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

		this.topic = "tick_stream";

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, 1);

		Map<String, List<KafkaStream<String, String>>> consumerMap = consumer.createMessageStreams(topicCountMap,
				new kafka.serializer.StringDecoder(null), new kafka.serializer.StringDecoder(null));
		List<KafkaStream<String, String>> streams = consumerMap.get(topic);

		KafkaStream<String, String> stream = streams.get(0);

		Executors.newSingleThreadExecutor().execute(new ConsumerThread(stream));
	}

	public class ConsumerThread implements Runnable {
		private KafkaStream<String, String> stream;

		public ConsumerThread(KafkaStream<String, String> stream) {
			this.stream = stream;
		}

		public void run() {
			ConsumerIterator<String, String> it = stream.iterator();
			while (it.hasNext()) {
				String message = it.next().message();

				TickData tickData = new TickData(message.substring(0, message.indexOf("#")), Double.parseDouble(message
						.substring(message.indexOf("#") + 1)));
				ArrayList<TickData> list = new ArrayList<TickData>(1);

				list.add(tickData);

				if (list.size() > 100) {
					try {
						queueTickData.put(new ArrayList<TickData>(list));
						list.clear();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				Log.debug("Got Message " + message);
				TOTAL_COUNT++;
			}
		}
	}

	public String getTicksGenerated() {
		return TOTAL_COUNT + "";
	}
}
