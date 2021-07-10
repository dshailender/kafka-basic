package com.github.dshailender.kafka.basics;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dshailender.kafka.config.AppConstants;

public class ProducerDemoWithCallBack {

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.BOOTSTRAP_SERVERS);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		for (int i = 0; i < 10; i++) {
			// create producer record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world " + Integer.toString(i));

			// send data
			producer.send(record, (recordMetadata, exception) -> {
				// executes every time a record is successfully sent or an exception is thrown
				if (null == exception) {
					// the record was successfully sent
					logger.info("Received new metadata \n" +
								"Topic: " + recordMetadata.topic() + "\n" +
								"Partition: "+ recordMetadata.partition() + "\n" +
								"Offset: " + recordMetadata.offset() + "\n"+
								"Timestamp: " + recordMetadata.timestamp());
				} else {
					logger.error("Error while producing", exception);
				}

			});
		}
		// flush data
		producer.flush();
		// flush and close producer
		producer.close();
	}

}
