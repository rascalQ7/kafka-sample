package kafka.publisher;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface PublishService {

  void publishDatasetAsString(Dataset<Row> dataset);
  void publishDatasetAsString(Dataset<Row> dataset, String delimiter);
  void publishDatasetAsJson(Dataset<Row> dataset);
}
