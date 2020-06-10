package net.jgp.books.spark.ch07.lab910_avro_ingestion

import org.apache.spark.sql.SparkSession

/**
 * Avro ingestion in a dataframe.
 *
 * Source of file: Apache Avro project -
 * https://github.com/apache/orc/tree/master/examples
 *
 * @author rambabu.posa
 */
object AvroToDataframeScalaApp {

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
      .appName("Avro to Dataframe")
      //.config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:2.4.5")
      .master("local[*]")
      .getOrCreate

    // Reads an Avro file, stores it in a dataframe
    val df = spark.read
      .format("avro")
      .load("data/weather.avro")

    // Shows at most 10 rows from the dataframe
    df.show(10)
    df.printSchema()
    println(s"The dataframe has ${df.count} rows.")

    spark.stop
  }

}
