from pyspark.sql import SparkSession

"""
COMMAND TO EXECUTE
spark-submit  application_example.py
"""

spark = SparkSession \
    .builder \
    .appName("NameAplication") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark Object is created ....")

spark.stop()