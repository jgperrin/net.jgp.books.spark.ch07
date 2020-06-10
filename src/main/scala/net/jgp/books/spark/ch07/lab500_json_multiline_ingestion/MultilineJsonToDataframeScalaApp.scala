package net.jgp.books.spark.ch07.lab500_json_multiline_ingestion

import org.apache.spark.sql.SparkSession

/**
 * Multiline ingestion JSON ingestion in a dataframe.
 *
 * @author rambabu.posa
 */
object MultilineJsonToDataframeScalaApp {

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
    val spark = SparkSession.builder
      .appName("Multiline JSON to Dataframe")
      .master("local[*]")
      .getOrCreate

    // Reads a JSON, called countrytravelinfo.json, stores it in a dataframe
    val df = spark.read
      .format("json")
      .option("multiline", true)
      .load("data/countrytravelinfo.json")

    // Shows at most 3 rows from the dataframe
    df.show(3)
    df.printSchema()

    spark.stop
  }

}
