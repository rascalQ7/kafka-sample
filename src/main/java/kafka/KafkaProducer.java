package kafka;

import java.util.Properties;

public class KafkaProducer {
  private static final String KAFKA_SERVER_URL = "localhost";
  private static final int KAFKA_SERVER_PORT = 9092;
  private static final String CLIENT_ID = "Producer";

  private KafkaProducer() {}

  public static org.apache.kafka.clients.producer.KafkaProducer getInstance() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
    properties.put("client.id", CLIENT_ID);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return new org.apache.kafka.clients.producer.KafkaProducer(properties);
  }
}
