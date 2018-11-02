package com.kafka.demo.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessageHeaderAccessor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.demo.model.ChatMessage;
import com.kafka.demo.services.KafkaProducer;
import com.kafka.demo.storage.MessageStorage;

@RestController
@RequestMapping(value = "/kafka/chat")
public class ChatController {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

	@Autowired
	KafkaProducer producer;

	@Autowired
	MessageStorage storage;
	
	/**
	 * Receiving message from Web Browser using STOMP CLIENT and further Sending
	 * message to a KAFKA TOPIC
	 * @param data
	 * @return
	 */
	@GetMapping(value = "/sendMessage")
	@MessageMapping("/sendMessage")
	public void sendMessage(ChatMessage message) throws Exception {
		logger.debug("ChatController.sendMessage : Received message from Web Browser using STOMP Client and further sending it to a KAFKA Topic");
		producer.send(ChatMessage.MessageType.valueOf(message.getType().name()) + "-" + message.getContent() + "-"
				+ message.getSender());
	}

	/**
	 * Adding username in Websocket
	 * @param chatMessage
	 * @param headerAccessor
	 * @return
	 */
	@MessageMapping("/chat.addUser")
	@SendTo("/topic/public")
	public ChatMessage addUser(@Payload ChatMessage chatMessage, SimpMessageHeaderAccessor headerAccessor) {
		// Add username in web socket session
		headerAccessor.getSessionAttributes().put("username", chatMessage.getSender());
		return chatMessage;
	}
	
	/**
	 * It consumes messages from a specified Topic
	 * @return
	 */
	@GetMapping(value = "/consumer")
	public String getAllRecievedMessage() {
		String messages = storage.toString();
		storage.clear();
		return messages;
	}
}