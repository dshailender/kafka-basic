package com.github.dshailender.kafka.basics;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dshailender.kafka.config.AppConstants;

public class ProducerDemoKeys {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.BOOTSTRAP_SERVERS);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		for (int i = 0; i < 10; i++) {
			// create producer record
			String topic = "first_topic";
			String value = "hello world " + Integer.toString(i);
			String key = "id_"+ Integer.toString(i);
			
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
			
			// send data
			producer.send(record, (recordMetadata, exception) -> {
				// executes every time a record is successfully sent or an exception is thrown
				if (null == exception) {
					// the record was successfully sent
					logger.info("Received new metadata \n" +
								"Topic: " + recordMetadata.topic() + "\n" +
								"Key: "+ key +" -> Partition: "+ recordMetadata.partition() + "\n" +
								"Offset: " + recordMetadata.offset() + "\n"+
								"Timestamp: " + recordMetadata.timestamp());
				} else {
					logger.error("Error while producing", exception);
				}

			}).get(); // block the send() to make it synchronous - don't do this in production
		}
		// flush data
		producer.flush();
		// flush and close producer
		producer.close();
	}

}
