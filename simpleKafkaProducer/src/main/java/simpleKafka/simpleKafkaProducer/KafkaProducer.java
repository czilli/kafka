package simpleKafka.simpleKafkaProducer;

import java.util.Properties;
import java.util.Scanner;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaProducer {
	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("metadata.broker.list", "localhost:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);
		Scanner scanner = new Scanner(System.in);
		KeyedMessage<String, String> data;
		boolean run = true;
		String input;
		try {
			while (run) {
				input = scanner.nextLine();

				if ("q".equals(input)) {
					run = false;
				}

				data = new KeyedMessage<String, String>("test", input);
				producer.send(data);
			}
		} finally {
			scanner.close();
			producer.close();
		}

	}
}
