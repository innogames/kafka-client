package com.innogames.analytics.kafka.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.innogames.analytics.kafka.exception.HandlerException;
import com.innogames.analytics.kafka.handler.Handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Consumer implements Runnable {

	private static final Logger logger = LogManager.getLogger();

	private final ConsumerContainerConfig consumerContainerConfig;
	private final List<Handler> handlers;
	private final KafkaConsumer<String, String> consumer;
	private final String uuid;
	private final AtomicBoolean closed;

	public Consumer(final ConsumerContainerConfig consumerContainerConfig) {
		this.consumerContainerConfig = consumerContainerConfig;
		this.handlers = createHandlers();
		this.consumer = new KafkaConsumer<>(consumerContainerConfig.getProperties());
		this.uuid = UUID.randomUUID().toString();
		this.closed = new AtomicBoolean(false);

		consumer.subscribe(consumerContainerConfig.getTopics());
		logger.info("Consumer[{}]: constructed with: {}", uuid, consumerContainerConfig.toString());
	}

	@Override
	public void run() {
		try {
			while(!closed.get()) {
				final ConsumerRecords<String, String> records = consumer.poll(100);

				for(final ConsumerRecord<String, String> record : records) {
					for(final Handler handler : handlers) {
						handler.onRecord(record);
					}
				}
			}
		} catch(final WakeupException e) {
			if(!closed.get()) {
				throw e;
			}
		} catch(final HandlerException e) {
			if(!closed.get()) {
				throw new RuntimeException(e);
			}
		} finally {
			consumer.close();
		}
	}

	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
		logger.info("Consumer[{}]: initiated shutdown", uuid);
	}

	private List<Handler> createHandlers() {
		final List<Handler> handlers = new ArrayList<>();

		handlers.addAll(consumerContainerConfig.getHandlers());
		this.consumerContainerConfig.getSuppliers().forEach(handlerSupplier -> handlers.add(handlerSupplier.get()));

		return handlers;
	}

}
