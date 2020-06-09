Please use the following steps to run this application

1. clone the project and cd to the folder
```
cd net.jgp.books.spark.ch07/src/main/python/lab910_avro_ingestion/
```

2. Run application
``` 
spark-submit --packages org.apache.spark:spark-avro_2.12:3.0.0-preview2 avroToDataframeApp.py
```