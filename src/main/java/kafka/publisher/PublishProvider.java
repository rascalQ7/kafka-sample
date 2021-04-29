package kafka.publisher;

import java.util.List;
import java.util.Objects;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class PublishProvider implements PublishService {

  private final Producer<String, String> producer;
  private final String topic;

  public PublishProvider(String topic, KafkaProducer kafkaProducer) {
    this.producer = Objects.requireNonNull(kafkaProducer);
    this.topic = Objects.requireNonNull(topic);
  }

  @Override
  public void publishDatasetAsJson(Dataset<Row> dataset) {
    List<String> aggregatedList = dataset.toJSON().collectAsList();
    aggregatedList.forEach(row -> producer.send(new ProducerRecord(topic, row)));
  }

  @Override
  public void publishDatasetAsString(Dataset<Row> dataset) {
    publishDatasetAsString(dataset, "");
  }

  @Override
  public void publishDatasetAsString(Dataset<Row> dataset, String delimiter) {
    List<Row> aggregatedList = dataset.collectAsList();
    aggregatedList.forEach(row ->
        producer.send(new ProducerRecord(topic, row.mkString(delimiter))));
  }
}
