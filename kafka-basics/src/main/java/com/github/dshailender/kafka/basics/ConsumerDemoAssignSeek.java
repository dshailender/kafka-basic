package com.github.dshailender.kafka.basics;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dshailender.kafka.config.AppConstants;

public class ConsumerDemoAssignSeek {

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
		final String topic = "first_topic";
		// create consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.BOOTSTRAP_SERVERS);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// create consumer
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

			// assign and seek are mostly used to replay data or fetch a specific message

			// assign
			TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
			consumer.assign(Collections.singleton(partitionToReadFrom));

			// seek
			consumer.seek(partitionToReadFrom, 15L);

			boolean keepOnReading = true;
			int numberOfMessagesToRead = 5;
			int numberOfMessagesReadSoFar = 0;

			// poll for new data
			while (keepOnReading) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : consumerRecords) {
					numberOfMessagesReadSoFar += 1;
					logger.info("Key: " + record.key() + " Value: " + record.value());
					logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
					if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
						keepOnReading = false; // to exit the while loop
						break; // to exit the for loop
					}
				}
			}

			logger.info("Exiting the application");
		}

	}

}
