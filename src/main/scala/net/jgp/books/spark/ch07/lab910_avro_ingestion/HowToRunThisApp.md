Please use the following steps to run this application

1. clone the project and package it
```
sbt clean assembly
```

2. Run application
``` 
spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.0-preview2 --class net.jgp.books.spark.ch07.lab910_avro_ingestion.AvroToDataframeScalaApp target/scala-2.12/SparkInAction2-Chapter07-assembly-1.0.0.jar
```