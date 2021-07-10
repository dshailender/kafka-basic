package com.github.dshailender.kafka.basics;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.github.dshailender.kafka.config.AppConstants;

public class ProducerDemo {

	public static void main(String[] args) {
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.BOOTSTRAP_SERVERS);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		// create producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");

		// send data
		producer.send(record);

		// flush data
		producer.flush();
		// flush and close producer
		producer.close();
	}

}
