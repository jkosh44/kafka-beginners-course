package producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

  public static void main(String[] args) throws ExecutionException, InterruptedException {

    Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    String bootstrapServers = "localhost:9092";

    // Create Producer properties
    Properties producerProps = new Properties();
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps
        .setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    // Create producer
    KafkaProducer<String,String> producer = new KafkaProducer(producerProps);

    for (int i = 0; i < 10; i++) {
      // Create producer record

      String topic = "first_topic";
      String value = "hello world " + i;
      String key = "id_" + i;
      ProducerRecord<String,String> record = new ProducerRecord<String,String>(topic, key, value);
      logger.info("Key: " + key);

      // send data
      producer.send(record, (recordMetadata, e) -> {
        //executes every time a record is successfully sent or an exception is thrown
        if (e == null) {
          //the record was successfully sent
          logger.info(String.format(
              "Received new metadata. \nTopic: %s \nPartition: %s \nOffset: %s \nTimestamp: %s",
              recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(),
              recordMetadata.timestamp()));
        } else {
          logger.error("Error while producing", e);
        }
      }).get();
    }

    producer.flush();
    producer.close();
  }
}
