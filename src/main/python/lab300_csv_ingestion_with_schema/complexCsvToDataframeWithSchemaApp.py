"""
 CSV ingestion in a dataframe with a Schema.

 @author rambabu.posa
"""
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField,
                               IntegerType,DateType,
                               StringType)

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/books.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Complex CSV with a schema to Dataframe") \
    .master("local[*]").getOrCreate()

# Creates the schema
schema = StructType([StructField('id', IntegerType(), False),
                     StructField('authorId', IntegerType(), True),
                     StructField('bookTitle', IntegerType(), False),
                     StructField('releaseDate', DateType(), True),
                     StructField('url', StringType(), False)])

# Reads a CSV file with header, called books.csv, stores it in a
# dataframe
df = spark.read.format("csv") \
    .option("header", True) \
    .option("multiline", True) \
    .option("sep", ";") \
    .option("dateFormat", "MM/dd/yyyy") \
    .option("quote", "*") \
    .schema(schema) \
    .load(absolute_file_path)


# Shows at most 20 rows from the dataframe
df.show(30, 25, False)
df.printSchema()

schemaAsJson = df.schema.json()
parsedSchemaAsJson = json.loads(schemaAsJson)

print("*** Schema as JSON: {}".format(json.dumps(parsedSchemaAsJson, indent=2)))

spark.stop()
