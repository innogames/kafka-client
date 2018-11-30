package com.innogames.analytics.kafka.example.consumer;

import com.innogames.analytics.kafka.consumer.ConsumerContainer;
import com.innogames.analytics.kafka.consumer.ConsumerContainerBuilder;
import com.innogames.analytics.kafka.exception.ConsumerContainerException;
import com.innogames.analytics.kafka.handler.NonBlockingHandler;

import java.util.Collections;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class App {

	public static void main(final String[] args) throws ConsumerContainerException {

		final ConsumerContainer consumerContainer = new ConsumerContainerBuilder()
			.setKafkaProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
			.setHandlers(Collections.singletonList(new NonBlockingHandler()))
			.setRandomGroupId()
			.setTopics(Collections.singletonList("test"))
			.setThreads(2)
			.build();

		consumerContainer.run();
		consumerContainer.shutdown();
	}

}
