package consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroups {

  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);

    String bootstrapServers = "localhost:9092";
    String groupId = "my-fifth-application";
    String topic = "first_topic";

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
    KafkaConsumer<String,String> consumer = new KafkaConsumer<>(consumerProperties);

    // subscribe consumer to our topic
    consumer.subscribe(Collections.singleton(topic));

    // poll for new data
    while (true) {
      ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

      for(ConsumerRecord record : records) {
        logger.info(String.format("Key: %s, Value: %s", record.key(), record.value()));
        logger.info((String.format("Partition: %s, Offset: %s", record.partition(), record.offset())));
      }
    }
  }
}
