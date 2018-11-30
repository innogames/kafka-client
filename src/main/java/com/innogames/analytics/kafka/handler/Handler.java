package com.innogames.analytics.kafka.handler;

import com.innogames.analytics.kafka.exception.HandlerException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface Handler {

	void onRecord(final ConsumerRecord<String, String> record) throws HandlerException;

}
