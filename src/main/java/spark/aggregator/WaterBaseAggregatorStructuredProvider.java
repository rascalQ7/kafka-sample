package spark.aggregator;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.substring;

import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class WaterBaseAggregatorStructuredProvider implements WaterBaseAggregatorService {

  private Dataset<Row> dataset;

  public WaterBaseAggregatorStructuredProvider(Dataset<Row> dataset) {
    this.dataset = Objects.requireNonNull(dataset);
  }

  @Override
  public Dataset<Row> getAllVolumesByCountry() {
    return dataset
        .groupBy(
            substring(col("monitoringSiteIdentifier"), 0, 2)
                .as("country"))
        .agg(count("country"));
  }
}
