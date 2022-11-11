from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Creating Spark session
spark = SparkSession.builder.appName("Problem3").getOrCreate()

# Defining schema since T has no headers
schema = StructType([
    StructField("transID", IntegerType(), False),
    StructField("custID", IntegerType(), False),
    StructField("transTotal", FloatType(), False),
    StructField("transNumItems", IntegerType(), False),
    StructField("transDesc", StringType(), False)
])

# Loading the dataset in, while fitting the schema
T = spark.read.csv("data/transactions.csv", header=False, schema=schema)
T.createOrReplaceTempView("transactions")


"""""
Manipulating data (Problem 3 parts 1-8)
"""""

# 1 - T1: Filter out (drop) the transactions from T whose total amount is less than $200
T1 = spark.sql("SELECT * FROM transactions WHERE transTotal >= 200")
# T1.orderBy("transTotal").show(10)

# 2 - T2: Over T1, group the transactions by the Number of Items it has, and for each group
# calculate the sum of total amounts, the average of total amounts, and the min and the
# max of the total amounts.
T1.createOrReplaceTempView("transactions")
T2 = spark.sql("SELECT transNumItems, sum(transTotal) as sum, avg(transTotal) as average, min(transTotal) as min, max(transTotal) as max "
               "FROM transactions "
               "GROUP BY transNumItems")
# T2.orderBy("transNumItems").show(10)

# 3 - Report back T2 to the client side
T2.write.csv("data/T2")

# 4 - T3: Over T1, group the transactions by customer ID, and for each group report the
# customer ID and the transactions’ count
T1.createOrReplaceTempView("transactions")
T3 = spark.sql("SELECT custID, count(ALL transID) as numTrans FROM transactions GROUP BY custID ")
# T3.orderBy("custID").show(10)

# 5 - T4: Filter out (drop) the transactions from T whose total amount is less than $600
T.createOrReplaceTempView("transactions")
T4 = spark.sql("SELECT * FROM transactions WHERE transTotal >= 600")
# T4.orderBy("transTotal").show(10)

# 6 - T5: Over T4, group the transactions by customer ID, and for each group report the
# customer ID, and the transactions’ count
T4.createOrReplaceTempView("transactions")
T5 = spark.sql("SELECT custID, count(ALL transID) as numTrans FROM transactions GROUP BY custID")
# T5.orderBy("custID").show(10)

# 7 - T6: Select the customer IDs whose T5.count * 5 < T3.count
T3.createOrReplaceTempView("T3")
T5.createOrReplaceTempView("T5")
T6 = spark.sql("SELECT T5.custID, T5.numTrans as trans2, T3.numTrans as trans1 "
               "FROM T5 join T3 ON T5.custID=T3.custID "
               "WHERE T5.numTrans < T3.numTrans")
# T6.orderBy("custID").show(20)

# 8 - Report back T6 to the client side
T6.write.csv("data/T6")

# Stopping Spark session
spark.stop()
