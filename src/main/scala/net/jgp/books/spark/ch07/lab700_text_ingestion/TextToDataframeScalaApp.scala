package net.jgp.books.spark.ch07.lab700_text_ingestion

import org.apache.spark.sql.SparkSession

/**
 * Text ingestion in a dataframe.
 *
 * Source of file: Rome & Juliet (Shakespeare) -
 * http://www.gutenberg.org/cache/epub/1777/pg1777.txt
 *
 * @author rambabu.posa
 */
object TextToDataframeScalaApp {

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
      .appName("Text to Dataframe")
      .master("local[*]")
      .getOrCreate

    // Reads a Romeo and Juliet (faster than you!), stores it in a dataframe
    val df = spark.read
      .format("text")
      .load("data/romeo-juliet-pg1777.txt")

    // Shows at most 10 rows from the dataframe
    df.show(10)
    df.printSchema

    spark.stop
  }

}
