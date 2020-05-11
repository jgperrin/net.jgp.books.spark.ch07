package net.jgp.books.spark.ch07.lab401_json_ingestion

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
 * JSON Lines ingestion in a dataframe.
 *
 * Note: This example is an extra, which is not described in chapter 7 of
 * the book: it illustrates how to access nested structure in the JSON
 * document directly. Linearization will be covered in chapter 12.
 *
 * For more details about the JSON Lines format, see: http://jsonlines.org/.
 *
 * @author rambabu.posa
 */
object JsonLinesToDataframeWithLinearizationScalaApp {

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
      .appName("JSON Lines to Dataframe")
      .master("local[*]")
      .getOrCreate

    // Reads a CSV file with header, called books.csv, stores it in a
    // dataframe
    var df = spark.read
      .format("json")
      .load("data/durham-nc-foreclosure-2006-2016.json")

    df = df.withColumn("year", col("fields.year"))
      .withColumn("coordinates", col("geometry.coordinates"))

    // Shows at most 5 rows from the dataframe
    df.show(5)
    df.printSchema()

    spark.stop
  }

}
