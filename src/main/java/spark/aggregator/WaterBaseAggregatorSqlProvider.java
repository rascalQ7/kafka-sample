package spark.aggregator;

import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class WaterBaseAggregatorSqlProvider implements WaterBaseAggregatorService {

  private final Dataset<Row> dataset;

  public WaterBaseAggregatorSqlProvider(Dataset<Row> dataset) {
    this.dataset = Objects.requireNonNull(dataset);
  }

  @Override
  public Dataset<Row> getAllVolumesByCountry() {
    final String VIEW = "waterbase";
    final String QUERY =
        "select substr(monitoringSiteIdentifier, 1, 2) as country, count(*) as volume"
            + " from " + VIEW
            + " group by country";

    dataset.createOrReplaceTempView(VIEW);
    return dataset.sqlContext().sql(QUERY);
  }
}
