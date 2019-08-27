package com.abhishek.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.abhishek.kafka.services.KafkaConsumer;
import com.abhishek.kafka.services.KafkaProducer;
import com.abhishek.kafka.storage.MessageStorage;

@RestController
@RequestMapping(value = "/jsa/kafka")
public class WebRestController {

	@Autowired
	KafkaProducer producer;

	@Autowired
	KafkaConsumer consumer;

	@Autowired
	MessageStorage storage;
	
	static int producercount=1;
	static int consumercount=1;

	@GetMapping(value = "/producer")
	public String producer(@RequestParam("data") String data) {
		for (int i = 1; i <11 ; i++) {
			producer.send(data +"--"+producercount);
			producercount++;
		}
		return "Done";
	}

	@GetMapping(value = "/consumer")
	public String getAllRecievedMessage() {

		String messages = storage.toString();
		consumer.processMessage(messages);
		
		storage.clear();

		return messages;
	}

}
