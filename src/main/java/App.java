import kafka.KafkaProducer;
import kafka.publisher.PublishProvider;
import kafka.publisher.PublishService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.Spark;
import spark.aggregator.WaterBaseAggregatorService;
import spark.aggregator.WaterBaseAggregatorSqlProvider;
import spark.aggregator.WaterBaseAggregatorStructuredProvider;

public class App {

  public static final String TOPIC = "seb-demo";
  public static final String INPUT_PATH = "src/main/resources/data/target/input.parquet";

  public static void main(String[] args) {
    SparkSession spark = Spark.getInstance();
    spark.sparkContext().setLogLevel("WARN");

    Dataset<Row> dataset = spark.read().parquet(INPUT_PATH);

    PublishService publisher = new PublishProvider(TOPIC, KafkaProducer.getInstance());

    WaterBaseAggregatorService wbSqlService = new WaterBaseAggregatorSqlProvider(dataset);
    WaterBaseAggregatorService wbStructService = new WaterBaseAggregatorStructuredProvider(dataset);

    Dataset<Row> aggregatedDataset = wbSqlService.getAllVolumesByCountry();
    publisher.publishDatasetAsString(aggregatedDataset, ": ");

    spark.stop();
  }
}
