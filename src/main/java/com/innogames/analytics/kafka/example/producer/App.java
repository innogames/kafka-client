package com.innogames.analytics.kafka.example.producer;

import com.innogames.analytics.kafka.producer.SimpleProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;

public class App {

	public static void main(final String[] args) {
		final SimpleProducer simpleProducer = new SimpleProducer();

		simpleProducer.start();
		simpleProducer.send(new ProducerRecord<>("test", String.format("example-%s", Instant.now().toString())));

		simpleProducer.stop();
	}

}
