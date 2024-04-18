# Databricks notebook source
/FileStore/tables/apple_products.csv

# COMMAND ----------

df= spark.read.format("csv").option("header","true").option("inferSchema","true").load("/FileStore/tables/apple_products.csv")

# COMMAND ----------

df.show(3)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

df.select("Sale Price").show(5)

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import *

# COMMAND ----------

df.select(max(col("Sale Price"))).show()
df.printSchema()
df.select(max(col("Mrp"))).show()
df.select(min(col("Mrp"))).show()

# COMMAND ----------

df.show(6)

# COMMAND ----------

df.select("Sale Price").show(1)
df.select("Mrp").show(1)

# COMMAND ----------

df.where("Mrp =49900").show()

df.where("Mrp =149900").show()

# COMMAND ----------

df.where("Mrp >49900").show(6)

# COMMAND ----------

#creating view so that we use Spark Sql

df.createOrReplaceTempView("Apple")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Apple 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from Apple 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `Product Name`, SUM(Mrp) AS sum_mrp 
# MAGIC FROM Apple 
# MAGIC GROUP BY `Product Name` 
# MAGIC HAVING sum_mrp > 80000
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT `Product Name`, SUM(Mrp) AS sum_mrp 
# MAGIC FROM Apple 
# MAGIC GROUP BY `Product Name` 
# MAGIC HAVING sum_mrp > 80000
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `Product Name`, SUM(Mrp) AS sum_mrp 
# MAGIC FROM Apple 
# MAGIC GROUP BY `Product Name` 
# MAGIC HAVING sum_mrp > 80000
# MAGIC ORDER BY sum_mrp DESC
# MAGIC

# COMMAND ----------

df.withColumn("Discounted_Price",col("Mrp")*0.1).show(5)

# COMMAND ----------

spark.sql("""SELECT 
                `Product Name`,
                SUM(Mrp) sum_mrp
          FROM Apple
          GROUP BY `Product Name`
          """).where("sum_mrp > 100000").show(6)

# COMMAND ----------

df.withColumn("dis_price", col("Mrp") * 0.1).select("Product Name","Mrp", "dis_price")\
.withColumn("new_price", col("Mrp")-col("dis_price"))\
.orderBy(col("new_price").desc())\
.show(10)

# COMMAND ----------


