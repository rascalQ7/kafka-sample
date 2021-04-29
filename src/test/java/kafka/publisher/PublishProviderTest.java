package kafka.publisher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import spark.Spark;

class PublishProviderTest {

  private static Dataset<Row> dataset;

  @Mock
  org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;

  private PublishService publisher;
  private AutoCloseable closeable;

  @BeforeAll
  static void beforeAll() {
    SparkSession spark = Spark.getInstance();
    List<Row> rows = Arrays.asList(RowFactory.create("test", "1"), RowFactory.create("test", "1"));
    StructType schema = DataTypes.createStructType(
        new StructField[]{
            DataTypes.createStructField("firstColumn", DataTypes.StringType, false),
            DataTypes.createStructField("secondColumn", DataTypes.StringType, false)}
    );
    dataset = spark.createDataFrame(rows, schema);
  }

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    publisher = new PublishProvider("topic", producer);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void shouldPublishDatasetAsString() {
    when(producer.send(any(ProducerRecord.class))).thenAnswer(
        invocationOnMock -> {
          assertEquals("test1",
              ((ProducerRecord) invocationOnMock.getArgument(0)).value());
          return null;
        }
    );

    publisher.publishDatasetAsString(dataset);

    verify(producer, times(2)).send(any(ProducerRecord.class));
  }

  @Test
  void shouldPublishDatasetAsStringWithDelimiter() {
    String delimiter = ", ";

    when(producer.send(any(ProducerRecord.class))).thenAnswer(
        invocationOnMock -> {
          assertEquals("test" + delimiter + "1",
              ((ProducerRecord) invocationOnMock.getArgument(0)).value());
          return null;
        }
    );

    publisher.publishDatasetAsString(dataset, delimiter);

    verify(producer, times(2)).send(any(ProducerRecord.class));
  }

  @Test
  void shouldPublishDatasetAsJson() {
    when(producer.send(any(ProducerRecord.class))).thenAnswer(
        invocationOnMock -> {
          assertEquals("{\"firstColumn\":\"test\",\"secondColumn\":\"1\"}",
              ((ProducerRecord) invocationOnMock.getArgument(0)).value());
          return null;
        }
    );

    publisher.publishDatasetAsJson(dataset);

    verify(producer, times(2)).send(any(ProducerRecord.class));
  }
}