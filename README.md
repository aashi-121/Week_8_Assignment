# NYC Yellow Taxi Trip Data Analysis using PySpark 🚖

## 📌 Objective

Load NYC Yellow Taxi Trip Data into **Azure Data Lake / Blob Storage / Databricks**, extract it into a **PySpark DataFrame**, and perform the following queries.

---

## 🔍 Questions

**Query 1**: Add a column named as `"Revenue"` into the dataframe which is the sum of:  
`fare_amount`, `extra`, `mta_tax`, `improvement_surcharge`, `tip_amount`, `tolls_amount`, `total_amount`

**Query 2**: Increasing count of total passengers in New York City by area.

**Query 3**: Real-time average fare / total earnings amount earned by 2 vendors.

**Query 4**: Moving count of payments made by each payment mode.

**Query 5**: Highest two gaining vendors on a particular date with number of passengers and total distance.

**Query 6**: Most number of passengers between a route of two locations.

**Query 7**: Top pickup locations with most passengers in last 5/10 seconds.

---

## ⚙️ Step 1: Setup & Data Load

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("NYC Taxi Data Analysis") \
    .getOrCreate()

df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .parquet("/mnt/nyc-taxi/yellow_tripdata_2018-01.parquet")

df.printSchema()
df.show(5)


✅ Query 1: Add Revenue Column
from pyspark.sql.functions import col

df = df.withColumn("Revenue",
    col("fare_amount") + col("extra") + col("mta_tax") +
    col("improvement_surcharge") + col("tip_amount") +
    col("tolls_amount") + col("total_amount")
)
df.select("Revenue").show(5)

✅ Query 2: Total Passengers by Pickup Area (Increasing Count)
from pyspark.sql.functions import round

df_area = df.withColumn("pickup_area", round(col("pickup_longitude"), 2).cast("string") + "," + round(col("pickup_latitude"), 2).cast("string"))

df_area.groupBy("pickup_area") \
    .sum("passenger_count") \
    .withColumnRenamed("sum(passenger_count)", "total_passengers") \
    .orderBy("total_passengers", ascending=False) \
    .show(10)

✅ Query 3: Average Fare / Total Earnings by Vendor
from pyspark.sql.functions import avg

df.groupBy("VendorID").agg(
    avg("fare_amount").alias("avg_fare"),
    avg("total_amount").alias("avg_total_earning")
).show()

✅ Query 4: Moving Count of Payments by Payment Mode
from pyspark.sql.window import Window
from pyspark.sql.functions import count

windowSpec = Window.partitionBy("payment_type").orderBy("tpep_pickup_datetime") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df = df.withColumn("moving_payment_count", count("payment_type").over(windowSpec))

df.select("payment_type", "tpep_pickup_datetime", "moving_payment_count").show(10)


✅ Query 5: Top 2 Earning Vendors on a Specific Date
from pyspark.sql.functions import to_date, sum

df = df.withColumn("trip_date", to_date("tpep_pickup_datetime"))

summary = df.groupBy("VendorID", "trip_date").agg(
    sum("passenger_count").alias("total_passengers"),
    sum("trip_distance").alias("total_distance"),
    sum("total_amount").alias("total_earning")
)

summary.orderBy("total_earning", ascending=False).show(2)


✅ Query 6: Most Passengers Between Route of Two Locations
from pyspark.sql.functions import concat_ws

df = df.withColumn("route", concat_ws("->", col("PULocationID"), col("DOLocationID")))

df.groupBy("route") \
    .sum("passenger_count") \
    .withColumnRenamed("sum(passenger_count)", "total_passengers") \
    .orderBy("total_passengers", ascending=False) \
    .show(1)

✅ Query 7: Top Pickup Locations with Most Passengers in Last 5/10 Seconds
from pyspark.sql.functions import unix_timestamp, current_timestamp

df = df.withColumn("pickup_unix", unix_timestamp("tpep_pickup_datetime"))

recent = df.filter((unix_timestamp(current_timestamp()) - col("pickup_unix")) <= 10)

recent.groupBy("PULocationID") \
    .sum("passenger_count") \
    .withColumnRenamed("sum(passenger_count)", "total_passengers") \
    .orderBy("total_passengers", ascending=False) \
    .show(10)


