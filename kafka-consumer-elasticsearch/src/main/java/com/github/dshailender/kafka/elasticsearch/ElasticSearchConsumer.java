package com.github.dshailender.kafka.elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.dshailender.kafka.config.AppConstants;
import com.google.gson.JsonParser;

public class ElasticSearchConsumer {

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

	public static RestHighLevelClient createClient() {
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(
				AppConstants.BONSAI_ELASTIC_SEARCH_ACCESS_KEY, AppConstants.BONSAI_ELASTIC_SEARCH_ACCESS_SECRET));

		RestClientBuilder restClientBuilder = RestClient
				.builder(new HttpHost(AppConstants.BONSAI_ELASTIC_SEARCH_FULL_ACCESS_URL, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					@Override
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
						return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});

		return new RestHighLevelClient(restClientBuilder);
	}

	public static KafkaConsumer<String, String> createConsumer(String topic) {
		final String groupId = "kafka-demo-elasticsearch";
		// create consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConstants.BOOTSTRAP_SERVERS);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // to disable auto commit of offsets
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		// subscribe consumer to our topic(s)
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}

	private static String extractIdFromTweet(String tweetJson) {
		return JsonParser.parseString(tweetJson).getAsJsonObject().get("id_str").getAsString();

	}

	public static void main(String[] args) throws IOException {
		RestHighLevelClient restHighLevelClient = createClient();

		KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");

		// poll for new data
		while (true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
			Integer recordCount = consumerRecords.count();
			logger.info("Recieved: " + recordCount + " records");
			BulkRequest bulkRequest = new BulkRequest();
			for (ConsumerRecord<String, String> record : consumerRecords) {
				// insert record into elastic search here
				try {
					IndexRequest indexRequest = new IndexRequest("twitter");
					indexRequest.id(extractIdFromTweet(record.value()));
					indexRequest.source(record.value(), XContentType.JSON);
					bulkRequest.add(indexRequest);
				} catch (NullPointerException e) {
					logger.warn("Skipping bad data " + record.value());
				}
				/*
				 * IndexResponse indexResponse = restHighLevelClient.index(indexRequest,
				 * RequestOptions.DEFAULT); logger.info(indexResponse.getId()); try {
				 * Thread.sleep(10); } catch (InterruptedException e) { e.printStackTrace(); }
				 */
			}
			if (recordCount > 0) {
				BulkResponse bulkItemResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
				logger.info("committing offsets...");
				consumer.commitSync();
				logger.info("Offsets have been committed");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
		// restHighLevelClient.close();
	}

}
