package com.kafka.demo.storage;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.kafka.demo.services.KafkaProducer;
 
@Component
public class MessageStorage {
	
	private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);
	
	private List<String> arralyList = new ArrayList<String>();
	
	public void add(String message){
		arralyList.add(message);
	}
	
	public String toString(){
		log.info("Calling MessageStorage.toString(");
		StringBuffer info = new StringBuffer();
		arralyList.forEach(msg->info.append(msg));
		log.info("info :"+info.toString());
		return info.toString();
	}
	
	public void clear(){
		arralyList.clear();
	}
}
