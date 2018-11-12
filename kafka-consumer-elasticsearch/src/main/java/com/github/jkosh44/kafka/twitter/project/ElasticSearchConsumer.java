package com.github.jkosh44.kafka.twitter.project;

import com.google.gson.JsonParser;
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
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {
  Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
  private String hostName;
  private String userName;
  private String password;

  public ElasticSearchConsumer() {
    Properties prop = new Properties();
    try (InputStream input = new FileInputStream(
        "/home/joe/IdeaProjects/kafka-beginners-course/kafka-consumer-elasticsearch/src/main/resources/elastic-search-secrets.properties")) {

      prop.load(input);
      hostName = prop.getProperty("access.url");
      userName = prop.getProperty("access.key");
      password = prop.getProperty("access.secret");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    new ElasticSearchConsumer().run();
  }

  public void run() throws IOException {
    RestHighLevelClient client = createClient();
    KafkaConsumer<String,String> consumer = createKafkaConsumer("smash_topic");

    while (true) {
      ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

      Integer recordCount = records.count();
      logger.info(String.format("Recieved %d records", recordCount));

      BulkRequest bulkRequest = new BulkRequest();

      for (ConsumerRecord<String,String> record : records) {
        if (record.value() != null) {
          //String id = String.format("%s_%s_%s", record.topic(), record.partition(), record.offset());
          try {
            String id = extractIdFromTweet(record.value());
            IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
                .source(record.value(), XContentType.JSON);

            bulkRequest.add(indexRequest);
          } catch (NullPointerException e) {
            logger.warn("skipping bad data: " + record.value());
          }
        }
      }
      if(recordCount > 0) {
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        logger.info("Committing offsets...");
        consumer.commitSync();
        logger.info("Offsets have been committed");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    //client.close();
  }

  public RestHighLevelClient createClient() {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider
        .setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
    RestClientBuilder builder = RestClient.builder(new HttpHost(hostName, 443, "https"))
        .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder
            .setDefaultCredentialsProvider(credentialsProvider));
    RestHighLevelClient client = new RestHighLevelClient(builder);
    return client;
  }

  public KafkaConsumer<String,String> createKafkaConsumer(String topic) {
    String bootstrapServers = "localhost:9092";
    String groupId = "kafka-demo-elasticsearch";

    // create consumer configs
    Properties consumerProperties = new Properties();
    consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProperties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

    // create consumer
    KafkaConsumer<String,String> consumer = new KafkaConsumer<>(consumerProperties);
    consumer.subscribe(Arrays.asList(topic));
    return consumer;
  }

  private String extractIdFromTweet(String tweetJson) {
    JsonParser jsonParser = new JsonParser();
    return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
  }
}
