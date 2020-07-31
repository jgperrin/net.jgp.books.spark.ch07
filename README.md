The examples in this repository are support to the **[Spark in Action, 2nd edition](http://jgp.net/sia)** book by Jean Georges Perrin and published by Manning. Find out more about the book on [Manning's website](http://jgp.ai/sia).

# Spark in Action, 2nd edition - chapter 7

Welcome to Spark with Java, chapter 7. This chapter covers file **ingestion from CSV, JSON, XML, text, and more**.

## Labs

Each chapter has one or more labs. Labs are examples used for teaching in the book(https://www.manning.com/books/spark-in-action-second-edition?a_aid=jgp). You are encouraged to take ownership of the code and modify it, experiment with it, hence the use of the term **lab**.

This code is designed to work with Apache Spark v3.0.0.

### Lab \#200

The `ComplexCsvToDataframeApp` application does the following:

1.	It acquires a session (a `SparkSession`).
2.	It asks Spark to load (ingest) a dataset in CSV format.
3.	It demonstrates how to add input options for the underlying data source.

### Lab \#201

Based on lab \#200, illustrates the push down filter().

This lab is not described in the [Spark in Action, 2nd edition](http://jgp.ai/sia). It was added in July 2020.

### Lab \#300

TBD

### Lab \#400

TBD

### Lab \#401

TBD

### Lab \#500

TBD

### Lab \#501

TBD

### Lab \#600

TBD

### Lab \#700

TBD

### Lab \#910

TBD

### Lab \#920

TBD

### Lab \#930

TBD

### Lab \#990

TBD

### Lab \#991

Based on video game sales prior to 2016, dataset from Kaggle (https://www.kaggle.com/gregorut/videogamesales).

This lab is not described in the [Spark in Action, 2nd edition](http://jgp.ai/sia). It was added in July 2020.

## Running the labs

### Java

For information on running the Java lab, see chapter 1 in [Spark in Action, 2nd edition](http://jgp.ai/sia).

### PySpark

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - "Spark in production: installation and a few tips").

Step by step direction for lab \#200. Follow similar steps for the other labs.

1. Clone this project

    git clone https://github.com/jgperrin/net.jgp.books.spark.ch07

2. Go to the lab in the Python directory

    cd net.jgp.books.spark.ch07/src/main/python/lab200_csv_ingestion/

3. Execute the following spark-submit command to create a jar file to our this application

    spark-submit complexCsvToDataframeApp.py

### Scala

You will need:
 * `git`.
 * Apache Spark (please refer Appendix P - "Spark in production: installation and a few tips"). 

Step by step direction for lab \#200. Follow similar steps for the other labs.

1. Clone this project

    git clone https://github.com/jgperrin/net.jgp.books.spark.ch07

2. Go to the right directory

    cd net.jgp.books.spark.ch07

3. Package application using sbt command

    sbt clean assembly

4. Run Spark/Scala application using spark-submit command as shown below:

    spark-submit --class net.jgp.books.spark.ch07.lab200_csv_ingestion.ComplexCsvToDataframeScalaApp target/scala-2.12/SparkInAction2-Chapter07-assembly-1.0.0.jar

Notes: 
 1. [Java] Due to renaming the packages to match more closely Java standards, this project is not in sync with the book's MEAP prior to v10 (published in April 2019).
 2. [Scala, Python] As of MEAP v14, we have introduced Scala and Python examples (published in October 2019).
 3. Erratas and additional examples are added and the labs may slightly differ from the once in the book (update in July 2020).
 
---

Follow me on Twitter to get updates about the book and Apache Spark: [@jgperrin](https://twitter.com/jgperrin). Join the book's community on [Facebook](https://www.facebook.com/SparkInAction/) or in [Manning's Live Book site](https://livebook.manning.com/book/spark-in-action-second-edition/about-this-book/?a_aid=jgp).
