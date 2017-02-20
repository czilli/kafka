package simpleKafka.simpleKafkaConsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class KafkaConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("group.id", "test_group");

		ConsumerConfig consumerConf = new ConsumerConfig(props);

		ConsumerConnector consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConf);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put("test", new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("test");
		ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();
		while (it.hasNext())
			System.out.println(new String(it.next().message()));
		consumer.shutdown();
	}
}
