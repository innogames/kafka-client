package com.innogames.analytics.kafka.consumer;

import com.innogames.analytics.kafka.handler.Handler;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

public class ConsumerContainerConfig {

	private Properties properties;
	private Integer threads;
	private List<String> topics;
	private List<Handler> handlers = new ArrayList<>();
	private List<Supplier<Handler>> handlerSuppliers = new ArrayList<>();

	public ConsumerContainerConfig() {
		properties = new Properties();

		properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

		threads = 1;
	}

	public Properties getProperties() {
		return properties;
	}

	public ConsumerContainerConfig setProperties(final Properties properties) {
		this.properties = properties;
		return this;
	}

	public Integer getThreads() {
		return threads;
	}

	public ConsumerContainerConfig setThreads(final Integer threads) {
		this.threads = threads;
		return this;
	}

	public List<String> getTopics() {
		return topics;
	}

	public ConsumerContainerConfig setTopics(final List<String> topics) {
		this.topics = topics;
		return this;
	}

	public List<Handler> getHandlers() {
		return handlers;
	}

	public ConsumerContainerConfig setHandlers(final List<Handler> handlers) {
		this.handlers = handlers;
		return this;
	}

	public List<Supplier<Handler>> getSuppliers() {
		return handlerSuppliers;
	}

	public ConsumerContainerConfig setSuppliers(final List<Supplier<Handler>> handlerSuppliers) {
		this.handlerSuppliers = handlerSuppliers;
		return this;
	}
}
