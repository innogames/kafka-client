package com.innogames.analytics.kafka.producer;


import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ProducerTest {

	private static final String TOPIC = "unit-test-topic";

	@Rule
	public KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());

	@Test
	public void testProducerProduceRecords() throws ExecutionException, InterruptedException {
		final int amountOfMessages = 10;

		final List<ProducerRecord<String, String>> records = new ArrayList<>();
		for(int i = 0; i < amountOfMessages; i++) {
			records.add(new ProducerRecord<>(TOPIC, String.format("example[%s]-%s", i, Instant.now().toString())));
		}

		final SimpleProducer simpleProducer = createProducer();

		simpleProducer.start();
		simpleProducer.send(records);
		simpleProducer.stop();

		final List<String> result = kafkaRule.helper().consumeStrings(TOPIC, amountOfMessages).get();

		assertThat(amountOfMessages, is(result.size()));
	}

	private SimpleProducer createProducer() {
		final Properties properties = new Properties();
		properties.put(BOOTSTRAP_SERVERS_CONFIG, String.format("localhost:%s", kafkaRule.helper().kafkaPort()));

		return new SimpleProducer(properties);
	}

}
