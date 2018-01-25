package net.jgp.books.sparkWithJava.ch07.lab_500.rdbms_ingestion;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MySQLToDatasetApp {

  public static void main(String[] args) {
    MySQLToDatasetApp app = new MySQLToDatasetApp();
    app.start();
  }

  private void start() {
    SparkSession spark = SparkSession.builder()
        .appName("Dataset from MySQL JDBC Connection")
        .master("local")
        .getOrCreate();

    java.util.Properties props = new Properties();
    props.put("user", "root");
    props.put("password", "password");
    props.put("useSSL", "false");
    
    Dataset<Row> df = spark.read().jdbc(
        "jdbc:mysql://localhost:3306/sakila?serverTimezone=EST",
        "actor", props);
    df = df.orderBy(df.col("last_name"));
    df.show();
  }
}
