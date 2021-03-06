package com.github.dshailender.kafka.basics;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dshailender.kafka.config.AppConstants;

public class ConsumerDemoGroups {

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);
		final String groupId = "my-fifth-application";
		final String topic = "first_topic";
		// create consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.BOOTSTRAP_SERVERS);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// create consumer
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {

			// subscribe consumer to our topic(s)
			consumer.subscribe(Arrays.asList(topic));

			// poll for new data
			while (true) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : consumerRecords) {
					logger.info("Key: " + record.key() + " Value: " + record.value());
					logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
				}
			}
		}

	}

}
