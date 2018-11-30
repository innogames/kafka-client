package com.innogames.analytics.kafka.consumer;

import com.innogames.analytics.kafka.exception.ConsumerContainerException;
import com.innogames.analytics.kafka.handler.Handler;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

public class ConsumerContainerBuilder {

	private ConsumerContainerConfig consumerContainerConfig;

	public ConsumerContainerBuilder() {
		consumerContainerConfig = new ConsumerContainerConfig();
		setRandomGroupId();
	}

	public ConsumerContainerBuilder(final ConsumerContainerConfig consumerContainerConfig) {
		this.consumerContainerConfig = consumerContainerConfig;
	}

	public ConsumerContainerBuilder setThreads(final Integer threads) {
		consumerContainerConfig.setThreads(threads);
		return this;
	}

	public ConsumerContainerBuilder setConsumerContainerConfig(final ConsumerContainerConfig consumerContainerConfig) {
		this.consumerContainerConfig = consumerContainerConfig;
		return this;
	}

	public ConsumerContainerBuilder setKafkaProperty(final String key, final String value) {
		consumerContainerConfig.getProperties().setProperty(key, value);
		return this;
	}

	public ConsumerContainerBuilder setGroupId(final String groupId) {
		consumerContainerConfig.getProperties().setProperty("group.id", groupId);
		return this;
	}

	public ConsumerContainerBuilder setRandomGroupId() {
		setGroupId(String.format("consumer-%s", UUID.randomUUID()));
		return this;
	}

	public ConsumerContainerBuilder setTopics(final List<String> topics) {
		consumerContainerConfig.setTopics(topics);
		return this;
	}

	public ConsumerContainerBuilder setHandlers(final List<Handler> handlers) {
		consumerContainerConfig.setHandlers(handlers);
		return this;
	}

	public ConsumerContainerBuilder setHandler(final Handler handler) {
		consumerContainerConfig.setHandlers(Collections.singletonList(handler));
		return this;
	}

	public ConsumerContainerBuilder setSuppliers(final List<Supplier<Handler>> suppliers) {
		consumerContainerConfig.setSuppliers(suppliers);
		return this;
	}

	public ConsumerContainerBuilder setSupplier(final Supplier<Handler> supplier) {
		consumerContainerConfig.setSuppliers(Collections.singletonList(supplier));
		return this;
	}

	public ConsumerContainerBuilder setBootstrapServers(final String bootstrapServers) {
		consumerContainerConfig.getProperties().setProperty("bootstrap.servers", bootstrapServers);
		return this;
	}

	public ConsumerContainerBuilder setAutoOffsetReset(final String autoOffsetReset) {
		consumerContainerConfig.getProperties().setProperty("auto.offset.reset", autoOffsetReset);
		return this;
	}

	public ConsumerContainer build() throws ConsumerContainerException {
		return new ConsumerContainer(consumerContainerConfig);
	}

}
