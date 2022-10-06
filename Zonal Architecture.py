# Databricks notebook source
# Raw Mount
dbutils.fs.mount(
  source = "wasbs://palashraw@palashgen2.blob.core.windows.net",
  mount_point = "/mnt/RawParquet",
  extra_configs = {"fs.azure.account.key.palashgen2.blob.core.windows.net":"rvFYDO/NszNWV0zRzBDd6Es+pTIYdoE+guzZwP0KOqwkqrVwlISMeXqSfxrea1a2Hym9lVs705kq+ASty9jyGQ=="})

# Silver Mount
dbutils.fs.mount(
  source = "wasbs://palashsilver@palashgen2.blob.core.windows.net",
  mount_point = "/mnt/SilverDelta",
  extra_configs = {"fs.azure.account.key.palashgen2.blob.core.windows.net":"rvFYDO/NszNWV0zRzBDd6Es+pTIYdoE+guzZwP0KOqwkqrVwlISMeXqSfxrea1a2Hym9lVs705kq+ASty9jyGQ=="})

# Gold Mount
dbutils.fs.mount(
  source = "wasbs://palashgold@palashgen2.blob.core.windows.net",
  mount_point = "/mnt/Gold",
  extra_configs = {"fs.azure.account.key.palashgen2.blob.core.windows.net":"rvFYDO/NszNWV0zRzBDd6Es+pTIYdoE+guzZwP0KOqwkqrVwlISMeXqSfxrea1a2Hym9lVs705kq+ASty9jyGQ=="})

# COMMAND ----------

# FilePaths
cD = "dbfs:/mnt/RawParquet/CustomerDim.parquet"
pD = "dbfs:/mnt/RawParquet/ProductDim.parquet"
rD = "dbfs:/mnt/RawParquet/RegionDim.parquet"
sF = "dbfs:/mnt/RawParquet/SaleFact.parquet"
tD = "dbfs:/mnt/RawParquet/TimeDim.parquet"
bF = "dbfs:/mnt/RawParquet/BudgetFact.parquet"

# Converted parquet to delta (From Raw to Silver)

cd = spark.read.format("parquet").load(cD)
cd.write.format("delta").mode("append").save("/mnt/SilverDelta/CustomerDim")

pd = spark.read.format("parquet").load(pD)
pd.write.format("delta").mode("append").save("/mnt/SilverDelta/ProductDim")

rd = spark.read.format("parquet").load(rD)
rd.write.format("delta").mode("append").save("/mnt/SilverDelta/RegionDim")

sf = spark.read.format("parquet").load(sF)
sf.write.format("delta").mode("append").save("/mnt/SilverDelta/SaleFact")

td = spark.read.format("parquet").load(tD)
td.write.format("delta").mode("append").save("/mnt/SilverDelta/TimeDim")

bf = spark.read.format("parquet").load(bF)
bf.write.format("delta").mode("append").save("/mnt/SilverDelta/BudgetFact")

# COMMAND ----------

display(cd)

# COMMAND ----------

# Manipulations in Python
from delta.tables import *
from pyspark.sql.functions import *

customerTable = DeltaTable.forPath(spark, "/mnt/SilverDelta/CustomerDim")

# Using a SQL-formatted string.
customerTable.update(
  condition = "Gender = 'F'",
  set = { "Gender": "'Female'" }
)

# Using Spark SQL functions.
customerTable.update(
  condition = col('Gender') == 'M',
  set = { 'Gender': lit('Male') }
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Manipulatons in SQL
# MAGIC UPDATE delta.`/mnt/SilverDelta/CustomerDim` SET MaritalStatus = 'Married' WHERE MaritalStatus = 'M';
# MAGIC UPDATE delta.`/mnt/SilverDelta/CustomerDim` SET MaritalStatus = 'Single' WHERE MaritalStatus = 'S';

# COMMAND ----------

# Load the Delta Tables from Silver.

cdTable= spark.read.format("delta").load("/mnt/SilverDelta/CustomerDim")
pdTable= spark.read.format("delta").load("/mnt/SilverDelta/ProductDim")
rdTable= spark.read.format("delta").load("/mnt/SilverDelta/RegionDim")
sfTable= spark.read.format("delta").load("/mnt/SilverDelta/SaleFact")
tdTable= spark.read.format("delta").load("/mnt/SilverDelta/TimeDim")
bfTable= spark.read.format("delta").load("/mnt/SilverDelta/BudgetFact")

# Manipulated Table stored in Gold
cdTable.write.format("delta").mode("Overwrite").save("/mnt/Gold/CustomerDim")
pdTable.write.format("delta").mode("Overwrite").save("/mnt/Gold/ProductDim")
rdTable.write.format("delta").mode("Overwrite").save("/mnt/Gold/RegionDim")
sfTable.write.format("delta").mode("Overwrite").save("/mnt/Gold/SaleFact")
tdTable.write.format("delta").mode("Overwrite").save("/mnt/Gold/TimeDim")
bfTable.write.format("delta").mode("Overwrite").save("/mnt/Gold/BudgetFact")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- History of manipulations on the file
# MAGIC 
# MAGIC DESCRIBE HISTORY "/mnt/SilverDelta/CustomerDim"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- Time-Travel by parameters like version number or timestamp
# MAGIC 
# MAGIC SELECT * FROM delta.`/mnt/SilverDelta/CustomerDim` version as of 4

# COMMAND ----------

# Load and Display Table from Gold
finalCustDim= spark.read.format("delta").load("/mnt/Gold/CustomerDim")
finalProdDim= spark.read.format("delta").load("/mnt/Gold/ProductDim")
finalRegDim= spark.read.format("delta").load("/mnt/Gold/RegionDim")
finalSaleFact= spark.read.format("delta").load("/mnt/Gold/SaleFact")
finalTimeDim= spark.read.format("delta").load("/mnt/Gold/TimeDim")
finalBudgetFact= spark.read.format("delta").load("/mnt/Gold/BudgetFact")

finalCustDim.write.mode("overwrite").saveAsTable("CustomerDim")
finalProdDim.write.mode("overwrite").saveAsTable("ProductDim")
finalRegDim.write.mode("overwrite").saveAsTable("RegionDim")
finalSaleFact.write.mode("overwrite").saveAsTable("SaleFact")
finalTimeDim.write.mode("overwrite").saveAsTable("TimeDim")
finalBudgetFact.write.mode("overwrite").saveAsTable("BudgetFact")

display(finalCustDim)
display(finalProdDim)
display(finalRegDim)
display(finalSaleFact)
display(finalTimeDim)
display(finalBudgetFact)

# COMMAND ----------

# To Unmount the Containers

dbutils.fs.unmount("/mnt/RawParquet")
dbutils.fs.unmount("/mnt/SilverDelta")
dbutils.fs.unmount("/mnt/Gold")

# COMMAND ----------

display(dbutils.fs.mounts())
