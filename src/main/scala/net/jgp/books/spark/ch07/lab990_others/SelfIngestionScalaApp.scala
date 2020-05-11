package net.jgp.books.spark.ch07.lab990_others

import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SparkSession}

/**
 * Simple ingestion followed by map and reduce operations.
 *
 * @author rambabu.posa
 */
object SelfIngestionScalaApp {

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
      .appName("Self ingestion")
      .master("local[*]")
      .getOrCreate


    val df = createDataframe(spark)
    df.show(false)

    // TODO: Convert below code into Spark(Scala) API
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
    val totalLinesL = df.selectExpr("sum(*)").first.getLong(0)
    println(totalLinesL)

    spark.stop

  }

  private def createDataframe(spark: SparkSession): DataFrame = {

    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("i", DataTypes.IntegerType, false)))

    val data = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    var rows = List[Row]()

    import scala.collection.JavaConversions._
    for (i <- data)
      rows = rows :+ RowFactory.create(int2Integer(i))

    spark.createDataFrame(rows, schema)
  }

}
