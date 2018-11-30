package com.innogames.analytics.kafka.consumer;


import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import com.innogames.analytics.kafka.exception.ConsumerContainerException;
import com.innogames.analytics.kafka.handler.BlockingHandler;
import com.innogames.analytics.kafka.handler.Handler;
import com.innogames.analytics.kafka.handler.NonBlockingHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ConsumerTest {

	private static final String TOPIC = "unit-test-topic";
	private static final Logger logger = LogManager.getLogger();

	@Rule
	public KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());

	@Test
	public void testConsumerEmitsEvents() throws ConsumerContainerException, InterruptedException {
		final AtomicLong count = new AtomicLong(0);
		final long amountOfMessages = 10;

		final List<Handler> handlers = Arrays.asList(
			new NonBlockingHandler(),
			new BlockingHandler(),
			record -> count.incrementAndGet()
		);

		final ConsumerContainer consumerContainer = new ConsumerContainerBuilder()
			.setHandlers(handlers)
			.setRandomGroupId()
			.setKafkaProperty(BOOTSTRAP_SERVERS_CONFIG, String.format("localhost:%s", kafkaRule.helper().kafkaPort()))
			.setTopics(Collections.singletonList(TOPIC))
			.setThreads(8)
			.build();

		consumerContainer.run();

		sleep(10000, "Give threads time to start");

		final List<String> messages = new ArrayList<>();
		for(long i = 0; i < amountOfMessages; i++) {
			messages.add(String.format("message%08d", i));
		}

		final String[] strings = messages
			.parallelStream()
			.toArray(String[]::new);

		kafkaRule
			.helper()
			.produceStrings(TOPIC, strings);

		sleep(10000, "Give threads time to consume");

		consumerContainer.shutdown();

		sleep(10000, "Give threads time to shutdown");

		assertThat(amountOfMessages, is(count.get()));
	}

	private void sleep(final int millis, final String message) throws InterruptedException {
		logger.info(message);
		Thread.sleep(millis);
	}

}
