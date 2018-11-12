package com.github.jkosh44.kafka.twitter.project;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
  Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
  private String consumerKey;
  private String consumerSecret;
  private String token;
  private String tokeySecret;

  public TwitterProducer() {
    Properties prop = new Properties();
    try(InputStream input = new FileInputStream("/home/joe/IdeaProjects/kafka-beginners-course/kafka-producer-twitter/src/main/resources/twitter-secret.properties")) {

      prop.load(input);
      consumerKey = prop.getProperty("consumer.key");
      consumerSecret = prop.getProperty("consumer.secret");
      token = prop.getProperty("token");
      tokeySecret = prop.getProperty("token.secret");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws InterruptedException {
    new TwitterProducer().run();
  }

  public void run() {
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
    Client hosebirdClient = createTwitterClient(msgQueue);



    // Attempts to establish a connection.
    hosebirdClient.connect();

    Producer<String, String> producer = createKafkaProducer();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Stopping app...");
      hosebirdClient.stop();
      producer.close();
      logger.info("App stopped");
    }));

    while(!hosebirdClient.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        hosebirdClient.stop();
      }
      if(msg != null) {
        // Create producer record
        ProducerRecord<String,String> record = new ProducerRecord<String,String>("smash_topic", msg);
        producer.send(record);
      }
    }

    hosebirdClient.stop();
    producer.flush();
    producer.close();
  }

  public Client createTwitterClient(BlockingQueue<String> msgQueue) {

    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

    List<String> terms = Arrays.asList("Smash Ultimate", "Melee", "Smash", "Super Smash Brothers");
    hosebirdEndpoint.trackTerms(terms);

    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokeySecret);

    ClientBuilder builder = new ClientBuilder()
        .name("Hosebird-Client-01")
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue));

    Client hosebirdClient = builder.build();
    return hosebirdClient;
  }

  public Producer<String, String> createKafkaProducer() {
    String bootstrapServers = "localhost:9092";

    // Create Producer properties
    Properties producerProps = new Properties();
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps
        .setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    producerProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    producerProps.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

    // Create producer
    KafkaProducer<String,String> producer = new KafkaProducer<>(producerProps);
    return producer;
  }
}
