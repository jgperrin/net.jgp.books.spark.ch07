package net.jgp.books.spark.ch07.lab930_parquet_ingestion

import org.apache.spark.sql.SparkSession

/**
 * Parquet ingestion in a dataframe.
 *
 * Source of file: Apache Parquet project -
 * https://github.com/apache/parquet-testing
 *
 * @author rambabu.posa
 */
object ParquetToDataframeScalaApp {

  /**
   * main() is your entry point to the application.
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {

    /**
     * The processing code.
     */
    // Creates a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Parquet to Dataframe")
      .master("local[*]")
      .getOrCreate

    // Reads a Parquet file, stores it in a dataframe
    val df = spark.read
      .format("parquet")
      .load("data/alltypes_plain.parquet")

    // Shows at most 10 rows from the dataframe
    df.show(10)
    df.printSchema()
    println(s"The dataframe has ${df.count} rows.")

    spark.stop
  }

}
