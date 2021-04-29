package spark.aggregator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import spark.Spark;

class WaterBaseAggregatorSqlProviderTest {

  private SparkSession spark;
  private WaterBaseAggregatorService aggregatorService;

  @Mock
  private Dataset<Row> dataset;

  private AutoCloseable closeable;

  @BeforeEach
  void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    aggregatorService = new WaterBaseAggregatorSqlProvider(dataset);
  }

  @AfterEach
  void tearDown() throws Exception {
    closeable.close();
  }

  @Test
  void name() {
  }
}