Traceback (most recent call last):
  File "/Users/ram/SparkInAction2Ed/Jan2020/net.jgp.books.spark.ch07/src/main/python/lab501_corrupt_json_multiline_ingestion/multilineJsonToDataframeWithCorruptRecordApp.py", line 47, in <module>
    df.show(3)
  File "/Users/ram/spark-3.0.0-preview2-bin-hadoop2.7/python/lib/pyspark.zip/pyspark/sql/dataframe.py", line 407, in show
  File "/Users/ram/spark-3.0.0-preview2-bin-hadoop2.7/python/lib/py4j-0.10.8.1-src.zip/py4j/java_gateway.py", line 1286, in __call__
  File "/Users/ram/spark-3.0.0-preview2-bin-hadoop2.7/python/lib/pyspark.zip/pyspark/sql/utils.py", line 102, in deco
pyspark.sql.utils.AnalysisException: Since Spark 2.3, the queries from raw JSON/CSV files are disallowed when the
referenced columns only include the internal corrupt record column
(named _corrupt_record by default). For example:
spark.read.schema(schema).json(file).filter($"_corrupt_record".isNotNull).count()
and spark.read.schema(schema).json(file).select("_corrupt_record").show().
Instead, you can cache or save the parsed results and then send the same query.
For example, val df = spark.read.schema(schema).json(file).cache() and then
df.filter($"_corrupt_record".isNotNull).count().;