# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://airlines@palashgen2.blob.core.windows.net",
  mount_point = "/mnt/airlines",
  extra_configs = {"fs.azure.account.key.palashgen2.blob.core.windows.net":"rvFYDO/NszNWV0zRzBDd6Es+pTIYdoE+guzZwP0KOqwkqrVwlISMeXqSfxrea1a2Hym9lVs705kq+ASty9jyGQ=="})

# COMMAND ----------

mF = "dbfs:/mnt/airlines/Raw_unprocessed/masterPartition"
mF = spark.read.format("parquet").load(mF)
mF.write.format("delta").option("mergeSchema", "true").mode("overwrite").save("/mnt/airlines/Silv_transformed/silverMaster")



# COMMAND ----------

display(mF)

# COMMAND ----------

from pyspark.sql.functions import *
df = spark.read.format("delta").load("/mnt/airlines/Silv_transformed/silverMaster").withColumn("DATE", lit(expr("DAY ||'-' || MONTH || '-' || YEAR")))

df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save("/mnt/airlines/Silv_transformed/silverMaster")



# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

ff = spark.read.format("delta").load("/mnt/airlines/Silv_transformed/silverMaster").drop("YEAR","MONTH","DAY")
ff.write.format("delta").option("mergeSchema", "true").mode("overwrite").save("/mnt/airlines/Silv_transformed/silverMaster")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC UPDATE delta.`/mnt/airlines/Silv_transformed/silverMaster` SET DAY_OF_WEEK = 'Monday' WHERE DAY_OF_WEEK = '1';
# MAGIC UPDATE delta.`/mnt/airlines/Silv_transformed/silverMaster` SET DAY_OF_WEEK = 'Tuesday' WHERE DAY_OF_WEEK = '2';
# MAGIC UPDATE delta.`/mnt/airlines/Silv_transformed/silverMaster` SET DAY_OF_WEEK = 'Wednesday' WHERE DAY_OF_WEEK = '3';
# MAGIC UPDATE delta.`/mnt/airlines/Silv_transformed/silverMaster` SET DAY_OF_WEEK = 'Thursday' WHERE DAY_OF_WEEK = '4';
# MAGIC UPDATE delta.`/mnt/airlines/Silv_transformed/silverMaster` SET DAY_OF_WEEK = 'Friday' WHERE DAY_OF_WEEK = '5';
# MAGIC UPDATE delta.`/mnt/airlines/Silv_transformed/silverMaster` SET DAY_OF_WEEK = 'Saturday' WHERE DAY_OF_WEEK = '6';
# MAGIC UPDATE delta.`/mnt/airlines/Silv_transformed/silverMaster` SET DAY_OF_WEEK = 'Sunday' WHERE DAY_OF_WEEK = '7';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- ALTER TABLE delta.`/mnt/airlines/Silv_transformed/silverMaster`  SET TBLPROPERTIES (
# MAGIC --    'delta.columnMapping.mode' = 'name',
# MAGIC --    'delta.minReaderVersion' = '2',
# MAGIC --    'delta.minWriterVersion' = '5')
# MAGIC    
# MAGIC ALTER TABLE delta.`/mnt/airlines/Silv_transformed/silverMaster` DROP COLUMNS (YEAR, MONTH, DAY) 

# COMMAND ----------

mTable= spark.read.format("delta").load("/mnt/airlines/Silv_transformed/silverMaster")
display(mTable)

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *
silverMaster = DeltaTable.forPath(spark, "/mnt/airlines/Silv_transformed/silverMaster")

silverMaster.update(
  condition = "CANCELLED = 0",
  set = { "CANCELLATION_REASON": "'None'" }
)


# COMMAND ----------

sM= spark.read.format("delta").load("/mnt/airlines/Silv_transformed/silverMaster")
sM.write.format("delta").mode("Overwrite").save("/mnt/airlines/Gol_finished/goldMaster")

# COMMAND ----------

display(sM)

# COMMAND ----------

gM = "dbfs:/mnt/airlines/Gol_finished/goldMaster"
fM= spark.read.format("delta").load(gM)
display(fM)

# COMMAND ----------

dbutils.fs.unmount("/mnt/airlines")
