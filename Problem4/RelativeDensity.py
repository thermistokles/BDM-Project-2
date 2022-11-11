from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Creating Spark session
spark = SparkSession.builder.appName("Problem4").getOrCreate()

# Defining schema since P has no headers
schema = StructType([
    StructField("x1", IntegerType(), False),
    StructField("y1", IntegerType(), False),
])

# Loading P into the Spark session
df = spark.read.csv("hdfs://localhost:9000/proj2/DataSetP_small.csv", header=False, schema=schema)
# df.orderBy("x1").show(20)

# Get relative density index
"""
- Add a column or function or whatever to calculate a point's cellID, or cell coords (easy)
- Calculate which 3-8 cellIDs/coordinate pairs are its neighbords
- Calculate relative density index
"""

# Report the neighbors of the TOP 50 grid cells
"""
- Get a df with cellID and RDI
- Get the top 50
- Get neighboring cellID's and report those for each of the 50
"""