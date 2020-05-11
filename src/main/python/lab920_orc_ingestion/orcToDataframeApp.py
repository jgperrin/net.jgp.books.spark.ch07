"""
 ORC ingestion in a dataframe.

 Source of file: Apache ORC project -
 https://github.com/apache/orc/tree/master/examples

 @author rambabu.posa
"""
from pyspark.sql import SparkSession
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/demo-11-zlib.orc"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("ORC to Dataframe") \
    .config("spark.sql.orc.impl", "native") \
    .master("local[*]").getOrCreate()

# Reads an ORC file, stores it in a dataframe
df = spark.read.format("orc") \
    .load(absolute_file_path)

# Shows at most 10 rows from the dataframe
df.show(10)
df.printSchema()

print("The dataframe has {} rows.".format(df.count()))

spark.stop()