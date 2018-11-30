package com.innogames.analytics.kafka.handler;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NonBlockingHandler implements Handler {

	private static final Logger logger = LogManager.getLogger();

	@Override
	public void onRecord(final ConsumerRecord<String, String> record) {
		logger.info(record);
	}

}
