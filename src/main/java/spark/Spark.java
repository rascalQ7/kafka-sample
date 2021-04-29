package spark;

import org.apache.spark.sql.SparkSession;

public class Spark {
  private static SparkSession instance = SparkSession
      .builder()
      .appName("spark.Spark app")
        .config("spark.master", "local")
        .getOrCreate();

  private Spark() {}

  public static SparkSession getInstance() {
    return instance;
  }
}
