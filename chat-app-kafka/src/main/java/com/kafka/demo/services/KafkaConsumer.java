package com.kafka.demo.services;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import com.kafka.demo.model.ChatMessage;
import com.kafka.demo.storage.MessageStorage;

@Service
public class KafkaConsumer {
	private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

	@Autowired
	MessageStorage storage;

	@Autowired
	private SimpMessagingTemplate template;

	@Value("${message-topic}")
	String kafkaTopic = "topic";

	/*@KafkaListener(topics = "${jsa.kafka.topic}")
	public void processMessage(String content) {
		log.info("received content = '{}'", content);
		storage.add(content);
	}*/

	/**
	 * Receives messages from a specified Topic and sending it to to Websocket.
	 * @param consumerRecord
	 * @throws Exception
	 */
	@KafkaListener(topics = "${message-topic}")
	public void consumer(ConsumerRecord<?, ?> consumerRecord) throws Exception {
		String[] message = consumerRecord.value().toString().split("-");
		log.info("Consumed data : '{}' from Kafka Topic : {}", Arrays.toString(message), kafkaTopic);
		storage.add(Arrays.toString(message)); // just to show message received from topic. not needed as such.
		// below line sends data to websocket i.e to web browser
		this.template.convertAndSend("/topic/public",
				new ChatMessage(ChatMessage.MessageType.valueOf(message[0]), message[1], message[2]));
	}
}