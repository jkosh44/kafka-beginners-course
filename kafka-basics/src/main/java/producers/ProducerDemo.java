package producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

  public static void main(String[] args) {
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

    // Create producer record
    ProducerRecord<String,String> record = new ProducerRecord<String,String>("first_topic",
        "hello world");

    // send data
    producer.send(record);

    producer.flush();
    producer.close();
  }
}