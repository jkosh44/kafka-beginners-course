package consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

  public static void main(String[] args) {
    new ConsumerDemoWithThreads().run();
  }

  public ConsumerDemoWithThreads() {
  }

  private void run() {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

    String bootstrapServers = "localhost:9092";
    String groupId = "my-sixth-application";
    String topic = "first_topic";
    CountDownLatch latch = new CountDownLatch(1);

    Runnable myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

    Thread myThread = new Thread(myConsumerRunnable);
    myThread.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Caught shutdown hook");
      ((ConsumerRunnable) myConsumerRunnable).shutdown();
      try {
        latch.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }));

    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.error("Application got interrupted", e);
    } finally {
      logger.info("Application finished");
    }
  }

  public class ConsumerRunnable implements Runnable {

    private CountDownLatch latch;
    private KafkaConsumer<String,String> consumer;
    private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    public ConsumerRunnable(String bootstrapServers, String groupId, String topic,
        CountDownLatch latch) {
      this.latch = latch;

      // create consumer configs
      Properties consumerProperties = new Properties();
      consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          StringDeserializer.class.getName());
      consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          StringDeserializer.class.getName());
      consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      // create consumer
      consumer = new KafkaConsumer<>(consumerProperties);

      // subscribe consumer to our topic
      consumer.subscribe(Collections.singleton(topic));
    }

    @Override public void run() {
      // poll for new data
      try {
        while (true) {
          ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

          for (ConsumerRecord record : records) {
            logger.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
            logger.info(
                (String.format("Partition: %s, Offset: %s", record.partition(), record.offset())));
          }
        }
      } catch (WakeupException e) {
        logger.info("Received shutdown signal!");
      } finally {
        consumer.close();
        latch.countDown();
      }
    }

    public void shutdown() {
      //Interups consumer.poll() with WakeUpException
      consumer.wakeup();
    }
  }
}
