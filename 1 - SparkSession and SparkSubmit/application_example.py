from pyspark.sql import SparkSession

"""
COMMAND TO EXECUTE
spark-submit  application_example.py
spark-submit --conf spark.sql.shuffle.partitions=300 application_example.py

"""

spark = SparkSession \
    .builder \
    .appName("NameAplication") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Number Partitions Shuffle: " + str(spark.conf.get("spark.sql.shuffle.partitions")))
print("Spark Object is created ....")

spark.stop()