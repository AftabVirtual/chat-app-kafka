package com.kafka.demo.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.simp.config.MessageBrokerRegistry;
import org.springframework.web.socket.config.annotation.*;

@Configuration
@EnableWebSocketMessageBroker
public class WebSocketConfig implements WebSocketMessageBrokerConfigurer {

	
	/**
	 * Registering a websocket endpoint that the clients will use to connect to our
	 * websocket server i.e. when a new user joins chat room.
	 */
    @Override
    public void registerStompEndpoints(StompEndpointRegistry registry) {
        registry.addEndpoint("/ws").withSockJS();
    }

	/**
	 * Messages whose destination starts with “/topic” should be routed to the
	 * message broker. Message broker broadcasts messages to all the connected
	 * clients who are subscribed to a particular topic.
	 */
    @Override
    public void configureMessageBroker(MessageBrokerRegistry registry) {
        registry.setApplicationDestinationPrefixes("/app");
        registry.enableSimpleBroker("/topic");
    }
    
}