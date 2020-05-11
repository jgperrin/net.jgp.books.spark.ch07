"""
 Simple ingestion followed by map and reduce operations.

 @author rambabu.posa
"""
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.types import IntegerType
import os

current_dir = os.path.dirname(__file__)
relative_path = "../../../../data/books.csv"
absolute_file_path = os.path.join(current_dir, relative_path)

# Creates a session on a local master
spark = SparkSession.builder.appName("Self ingestion") \
    .master("local[*]").getOrCreate()


def createDataframe(spark):
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    df = spark.createDataFrame(data, IntegerType())
    return df


df = createDataframe(spark)
df.show(20)

# SQL-like
totalLinesL = df.selectExpr("sum(*)").first()[0]
print(totalLinesL)

spark.stop()