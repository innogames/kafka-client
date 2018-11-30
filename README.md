# Multithreaded Kafka Client

This library offers an easy way to consume messages from Kafka using more then a single thread by creating a container with consumer.
For more details about Kafka, you can have a look at 
the [Documentation](https://kafka.apache.org/). 


## Basic usage
The `KafkaConsumerBuilder` offers an easy way to create a container with consumers. It is
very straightforward to use:

```java
final ConsumerContainer consumerContainer = new ConsumerContainerBuilder()
	.setHandlers(Collections.singletonList(new NonBlockingHandler()))
	.setKafkaProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
	.setRandomGroupId()
	.setTopics(Collections.singletonList("unit-test-topic"))
	.setThreads(2)
	.build();
```

The consumer can be started by using the `run()` method:
```java
consumerContainer.run();
```

To stop the consumer, call its `shutdown()` method. This will cause the 
consumer to not fetch more messages. Messages that are already in the
 buffer will be processed before the consumer stops.
```java
consumerContainer.shutdown();
```

## Features

### Threads
This library is able to process different Kafka streams in parallel 
using threads. Message handlers are isolated and do not share state. That means, if two
threads are used, the handler will not be synchronized. To share state use atomics or synchronize the access (see the BlockingHandler as example).

### Group Id
The group id of the consumers is used to store the state (the current 
offset). Please do not use a group id that could interfere with existing
group ids. The best is to talk to the dev.Analytics team to get a new group id.
There is also the option to use a random group id. This will lead the
consumer to consume all existing messages from the beginning. This
behavior can be adjusted so that the consumer reads only new messages.

### Topics
The data in Kafka is organized in topics which containing all events which are sent to our event
gateways.

### Handler
The implementation effort to handle/process messages is very low. There
is an interface called `Handler`.
For each consumed message, the `onRecord()` method is called with a
`ConsumerRecord` as argument. Use `ConsumerRecord#value()` to access the
actual data within the message.

## Example
A full example can be found in `/src/main/java/com/innogames/analytics/kafka/example/`.

## How to Contribute
If you have suggestions or improvements, feel free to contribute to the
library.

## Notes
* The consumer uses a fixed buffer size for events. If the handler is
  slow, it can take a while until this buffer is completely processed.
  This can cause delays while shutting down the consumer. This is a 
  limitation in the Kafka consumer implementation itself.
