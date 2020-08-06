package net.jgp.books.spark.ch07.lab991_pushdown_filter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * CSV ingestion in a dataframe.
 * 
 * @author jgp
 */
public class PushdownCsvFilterApp {

  enum Mode {
    NO_FILTER,  FILTER
  }

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
    PushdownCsvFilterApp app = new PushdownCsvFilterApp();
    //app.buildBigCsvFile();
    // app.start(Mode.NO_FILTER);
    app.start(Mode.FILTER);
  }

  private void buildBigCsvFile() {
    System.out.println("Build big CSV file");
    SparkSession spark = SparkSession.builder()
        .appName("Pushdown CSV filter")
        .config("spark.sql.csv.filterPushdown.enabled", false)
        .master("local[*]")
        .getOrCreate();

    System.out.println("Read initial CSV");
    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load("data/VideoGameSales/vgsales.csv");

    System.out.println("Increasing number of record 10x");
    for (int i = 1; i < 11; i++) {
      System.out.println("Increasing number of record, step #" + i);
      df = df.union(df);
    }

    System.out.println("Saving big CSV file");
    df = df.coalesce(1);
    df.write()
        .format("csv")
        .option("header", true)
        .mode(SaveMode.Overwrite)
        .save("/tmp/vgasales/csv");
    spark.stop();
    System.out.println("Big CSV file ready");
  }

  /**
   * The processing code.
   * 
   * @param filter
   */
  private void start(Mode filter) {
    SparkSession spark = null;
    Dataset<Row> df = null;

    long t0 = System.currentTimeMillis();
    switch (filter) {
      case NO_FILTER:
        spark = SparkSession.builder()
            .appName("Pushdown CSV filter")
            .config("spark.sql.csv.filterPushdown.enabled", false)
            .master("local[*]")
            .getOrCreate();
        System.out.println("Using Apache Spark v" + spark.version());
        df = ingestionWithoutFilter(spark);
        break;

      case FILTER:
        spark = SparkSession.builder()
            .appName("Pushdown CSV filter")
            .config("spark.sql.csv.filterPushdown.enabled", true)
            .master("local[*]")
            .getOrCreate();
        System.out.println("Using Apache Spark v" + spark.version());
        df = ingestionWithFilter(spark);
        break;
    }

    df.explain("formatted");
    df.write().format("parquet").mode(SaveMode.Overwrite)
        .save("/tmp/vgasales/" + filter);
    long t1 = System.currentTimeMillis();
    spark.stop();
    System.out
        .println("Operation " + filter + " tool " + (t1 - t0) + " ms.");
  }

  private Dataset<Row> ingestionWithFilter(SparkSession spark) {
    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load("/tmp/vgasales/csv/*.csv")
        .filter("Platform = 'Wii'");
    return df;
  }

  private Dataset<Row> ingestionWithoutFilter(SparkSession spark) {
    Dataset<Row> df = spark.read().format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load("/tmp/vgasales/csv/*.csv");
    return df;
  }
}
