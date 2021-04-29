package spark.aggregator;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface WaterBaseAggregatorService {
  Dataset<Row> getAllVolumesByCountry();
}
