# Databricks notebook source
#Report dataframe
reportDF = sql("""
select 
  taxi_type,trip_year,pickup_hour,count(*) as trip_count
from 
  taxi_db.taxi_trips_mat_view
group by 
  taxi_type,trip_year,pickup_hour
""")

# COMMAND ----------

# MAGIC %run ./GlobalVarsAndMethods

# COMMAND ----------

#Persist predictions to destination RDBMS
reportDF.coalesce(1).write.jdbc(jdbcUrl, "TRIPS_BY_HOUR", mode="overwrite", properties=connectionProperties)

# COMMAND ----------

dbutils.notebook.exit("Pass")
