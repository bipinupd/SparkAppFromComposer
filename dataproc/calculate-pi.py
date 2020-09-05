from pyspark.sql import SparkSession
from random import random
import sys

def inside(p):
    x, y = random(), random()
    return x*x + y*y < 1

NUM_SAMPLES = int(sys.argv[1])
spark = SparkSession \
    .builder \
    .appName("CalculatePi") \
    .getOrCreate()

sc = spark.sparkContext

count = sc.parallelize(range(0, NUM_SAMPLES)) \
             .filter(inside).count()
print("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))