package net.jgp.books.spark.ch07.lab990_others;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Simple ingestion followed by map and reduce operations.
 * 
 * @author jgp
 */
public class SelfIngestionApp {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    SelfIngestionApp app = new SelfIngestionApp();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("Self ingestion")
        .master("local[*]")
        .getOrCreate();

    Dataset<Row> df = createDataframe(spark);
    df.show(false);

    // map and reduce with getAs()
    // The following code does not work (yet) with Spark 3.0.0 (preview 1)
    // int totalLines = df
    // .map(
    // (MapFunction<Row, Integer>) row -> row.<Integer>getAs("i"),
    // Encoders.INT())
    // .reduce((a, b) -> a + b);
    // System.out.println(totalLines);

    // map and reduce with getInt()
    // The following code does not work (yet) with Spark 3.0.0 (preview 1)
    // totalLines = df
    // .map(
    // (MapFunction<Row, Integer>) row -> row.getInt(0),
    // Encoders.INT())
    // .reduce((a, b) -> a + b);
    // System.out.println(totalLines);

    // SQL-like
    long totalLinesL = df.selectExpr("sum(*)").first().getLong(0);
    System.out.println(totalLinesL);
  }

  private static Dataset<Row> createDataframe(SparkSession spark) {
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(
            "i",
            DataTypes.IntegerType,
            false) });

    List<Integer> data =
        Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    List<Row> rows = new ArrayList<>();
    for (int i : data) {
      rows.add(RowFactory.create(i));
    }

    return spark.createDataFrame(rows, schema);
  }
}
