package com.kafka.xav.kafka_next_demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoCallback {

	public static void main(String[] args) {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoCallback.class);

		String bootstrapServers = "127.0.0.1:9092";

		// create Producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int i = 0; i < 10; i++) {
			// create a producer record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("topic-1",
					"ABCDE " + Integer.toString(i));

			// send data - asynchronous
			producer.send(record, new Callback() {
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					// executes every time a record is successfully sent or an exception is thrown
					if (e == null) {
						// the record was successfully sent
						System.out.println("Received new metadata. \n" + "Topic:" + recordMetadata.topic() + "\n"
								+ "Partition: " + recordMetadata.partition() + "\n" + "Offset: "
								+ recordMetadata.offset() + "\n" + "Timestamp: " + recordMetadata.timestamp());
						
					} else {
						logger.error("Error while producing", e);
					}
				}
			});
		}

		// flush data
		producer.flush();
		// flush and close producer
		producer.close();

	}
}
