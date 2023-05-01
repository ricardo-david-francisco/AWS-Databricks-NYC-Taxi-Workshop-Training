# Databricks notebook source
# MAGIC %md
# MAGIC # What's in this exercise?
# MAGIC We will run various reports and visualize

# COMMAND ----------

# MAGIC %md
# MAGIC # Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show a few records

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.  Trip count by pickup_borough

# COMMAND ----------

# MAGIC %sql
# MAGIC -- use training;
# MAGIC select
# MAGIC   pickup_borough,
# MAGIC   count(*) as trip_count
# MAGIC from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated
# MAGIC   -- taxinyc_trips.taxi_trips_mat_view
# MAGIC group by pickup_borough

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Excercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Count trips by passenger_count
# MAGIC Order ascending by passenger_count

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   passenger_count, count(*) as trip_count
# MAGIC from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated
# MAGIC group by passenger_count
# MAGIC order by passenger_count asc

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.  Total revenue including tips
# MAGIC use `total_amount` to get revenue incl tips

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   sum(total_amount) revenue
# MAGIC from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.  Revenue share by pickup_borough

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   pickup_borough, sum(total_amount) revenue
# MAGIC from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated
# MAGIC group by pickup_borough

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.  Revenue share by pickup_borough
# MAGIC
# MAGIC But count pickup_borough, discard trips which pick_borough is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   pickup_borough, sum(total_amount) revenue
# MAGIC from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated
# MAGIC where pickup_borough is not null
# MAGIC group by pickup_borough

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.  Trip count trend by month, by pickup_borough, for 2016

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   pickup_borough,
# MAGIC   trip_month as month,
# MAGIC   count(*) as trip_count
# MAGIC from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated
# MAGIC where 
# MAGIC   trip_year=2016
# MAGIC group by pickup_borough,trip_month
# MAGIC order by trip_month 

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.  Average trip distance by pickup_borough
# MAGIC
# MAGIC Use functions `avg()` for average, and `round()` to round off value.
# MAGIC
# MAGIC Use `trip_distance`.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   pickup_borough, round(avg(trip_distance),2) as trip_distance_miles
# MAGIC from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated
# MAGIC group by pickup_borough

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.  Average trip amount by pickup_borough
# MAGIC
# MAGIC Use `total_amount`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   pickup_borough, round(avg(total_amount),2) as avg_total_amount
# MAGIC from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated
# MAGIC group by pickup_borough

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Count with no tip, by pickup_borough
# MAGIC `tip_amount` is the tip column

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   pickup_borough, count(*) tipless_count
# MAGIC from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated
# MAGIC where tip_amount=0
# MAGIC group by pickup_borough

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Count trips with no charge, by pickup_borough
# MAGIC
# MAGIC `Payment_type`
# MAGIC
# MAGIC A numeric code signifying how the passenger paid for the trip. Type values:
# MAGIC
# MAGIC 1. `Credit card`
# MAGIC 2. `Cash`
# MAGIC 3. `No charge`
# MAGIC 4. `Dispute`
# MAGIC 5. `Unknown`
# MAGIC 6. `Voided trip`

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   pickup_borough, count(*) as transactions
# MAGIC from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated
# MAGIC where
# MAGIC   payment_type=3
# MAGIC   and total_amount=0.0
# MAGIC group by pickup_borough

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Count trips by payment type

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select
# MAGIC   payment_type, count(*) as transactions
# MAGIC from
# MAGIC   training.taxinyc_trips.yellow_taxi_trips_curated
# MAGIC group by payment_type

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Count trips by pickup hour in 2016
# MAGIC
# MAGIC Count by pickup_hour for year 2016

# COMMAND ----------

# MAGIC %sql
# MAGIC select pickup_hour,count(*) as trip_count
# MAGIC from training.taxinyc_trips.yellow_taxi_trips_curated
# MAGIC where trip_year=2016
# MAGIC group by pickup_hour
# MAGIC order by pickup_hour

# COMMAND ----------

# MAGIC %md
# MAGIC # Bonus exercises

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12.  Top 3 pickup-dropoff zones for 2016

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from
# MAGIC   (
# MAGIC   select
# MAGIC     pickup_zone,dropoff_zone,count(*) as trip_count
# MAGIC   from
# MAGIC     training.taxinyc_trips.yellow_taxi_trips_curated
# MAGIC   where
# MAGIC     trip_year=2016
# MAGIC   and
# MAGIC     pickup_zone is not null and pickup_zone<>'NV'
# MAGIC   and
# MAGIC     dropoff_zone is not null and dropoff_zone<>'NV'
# MAGIC   group by pickup_zone,dropoff_zone
# MAGIC   order by trip_count desc
# MAGIC   ) x
# MAGIC limit 3

# COMMAND ----------


