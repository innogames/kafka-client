package com.innogames.analytics.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class SimpleProducer {

	private static final Logger logger = LogManager.getLogger();

	private final Properties properties;

	private Producer<String, String> producer;

	public SimpleProducer() {
		this(null);
	}

	public SimpleProducer(final Properties custom) {
		properties = new Properties();

		properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ACKS_CONFIG, "all");
		properties.put(BUFFER_MEMORY_CONFIG, 33554432);
		properties.put(COMPRESSION_TYPE_CONFIG, "snappy");
		properties.put(RETRIES_CONFIG, 3);
		properties.put(BATCH_SIZE_CONFIG, 16384);
		properties.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		properties.putAll(custom);
	}

	public void start() {
		logger.info("Starting producer with properties {}", properties);

		producer = new KafkaProducer<>(properties);
	}

	public void stop() {
		producer.flush();
		producer.close();
	}

	public void send(final List<ProducerRecord<String, String>> records) {
		records.forEach(this::send);
	}

	public Future send(final ProducerRecord<String, String> record) {
		return producer.send(record);
	}

}
