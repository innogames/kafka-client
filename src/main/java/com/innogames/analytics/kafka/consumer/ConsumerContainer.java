package com.innogames.analytics.kafka.consumer;

import com.innogames.analytics.kafka.exception.ConsumerContainerException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerContainer {

	private static final Logger logger = LogManager.getLogger();

	private final ConsumerContainerConfig consumerContainerConfig;
	private final List<Consumer> consumers;
	private final ExecutorService executor;

	public ConsumerContainer(final ConsumerContainerConfig consumerContainerConfig) throws ConsumerContainerException {
		if(!consumerContainerConfig.getProperties().containsKey("group.id")) {
			throw new ConsumerContainerException("group.id not set");
		}

		this.consumerContainerConfig = consumerContainerConfig;
		this.consumers = new ArrayList<>();
		this.executor = Executors.newFixedThreadPool(consumerContainerConfig.getThreads());

		logger.info("Set up ConsumerContainer with: {}", consumerContainerConfig);
	}

	public void run() {
		for(int i = 0; i < consumerContainerConfig.getThreads(); i++) {
			consumers.add(new Consumer(consumerContainerConfig));
		}

		consumers.forEach(executor::execute);
	}

	public void shutdown() {
		consumers.forEach(Consumer::shutdown);
	}

}
