package com.github.dshailender.kafka.basics;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dshailender.kafka.config.AppConstants;

public class ConsumerDemoWithThread {

	public static void main(String[] args) {

		new ConsumerDemoWithThread().run();

	}

	private ConsumerDemoWithThread() {

	}

	private void run() {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
		final String groupId = "my-sixth-application";
		final String topic = "first_topic";

		// latch for dealing with multiple threads
		final CountDownLatch latch = new CountDownLatch(1);

		// creating the consumer runnable
		logger.info("creating the consumer thread");
		Runnable myConsumerRunnable = new ConsumerRunnable(groupId, topic, latch);

		// start the thread
		Thread myThread = new Thread(myConsumerRunnable);
		myThread.start();

		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Caught shotdown hook");
			((ConsumerRunnable) myConsumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				logger.error("shotdown hook got interrupted", e);
			} finally {
				logger.info("Application has exited");
			}
		}));

		try {
			latch.await();
		} catch (InterruptedException e) {
			logger.error("Application got interrupted", e);
		} finally {
			logger.info("Application is closing");
		}
	}

	public class ConsumerRunnable implements Runnable {

		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

		public ConsumerRunnable(String groupId, String topic, CountDownLatch latch) {
			// create consumer properties
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.BOOTSTRAP_SERVERS);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			// set fields and create consumer
			this.latch = latch;
			this.consumer = new KafkaConsumer<>(properties);
			// subscribe consumer to our topic(s)
			this.consumer.subscribe(Arrays.asList(topic));
		}

		@Override
		public void run() {
			try {
				// poll for new data
				while (true) {
					ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
					for (ConsumerRecord<String, String> record : consumerRecords) {
						logger.info("Key: " + record.key() + " Value: " + record.value());
						logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
					}
				}
			} catch (WakeupException e) {
				logger.info("Received shutdown signal");
			} finally {
				consumer.close();
				// tell our main code we're done with consumer
				latch.countDown();
			}

		}

		public void shutdown() {
			// the wakeup method is special method to interrupt consumer.poll()
			// it will throw WakeupException
			consumer.wakeup();
		}

	}

}
