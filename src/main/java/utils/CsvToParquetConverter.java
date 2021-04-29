package utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import spark.Spark;

/**
 * CSV to parquet converter
 */
public class CsvToParquetConverter {

  public static void main(String[] args) {

    SparkSession spark = Spark.getInstance();
    spark.sparkContext().setLogLevel("WARN");

    Dataset<Row> waterBaseDataFrame = spark
        .read()
        .csv("src/main/resources/data/initial/Waterbase_v2018_1_T_WISE4_AggregatedData.csv");

    waterBaseDataFrame.write().mode(SaveMode.Overwrite)
        .parquet("src/main/resources/data/target/test.parquet");

    spark.stop();
  }
}
