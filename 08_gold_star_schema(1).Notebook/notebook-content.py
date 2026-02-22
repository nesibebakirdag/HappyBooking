# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "300fcea7-917c-46f4-b882-033c12af9963",
# META       "default_lakehouse_name": "lh_happybooking",
# META       "default_lakehouse_workspace_id": "c3fa9526-c341-4b89-a11c-94170caf1f28",
# META       "known_lakehouses": [
# META         {
# META           "id": "300fcea7-917c-46f4-b882-033c12af9963"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Gold Layer: Star Schema (dbt Logic as PySpark)
# 
# **Amaç:** Silver'daki temiz binlerce tablolardan Power BI'a hazır Star Schema oluştur.
# 
# | Tablo | Tip | Açıklama |
# |-------|-----|----------|
# | `gold_fact_bookings` | Fact | Booking transaction + EUR KPI |
# | `gold_dim_city` | Dim | City/Country + weather + exchange rates |
# | `gold_dim_hotel` | Dim | Hotel attributes + tier |
# | `gold_dim_date` | Dim | Calendar (Time Intelligence) |
# 
# **Not:** Bu notebook, `dbt_project/` içindeki SQL modellerin PySpark karşılığıdır.

# CELL ********************

from pyspark.sql.functions import (col, year, month, dayofmonth, quarter, dayofweek,
                                    weekofyear, dayofyear, date_format, when, lit,
                                    coalesce, round as spark_round, first, row_number,
                                    explode, sequence, to_date, expr)
from pyspark.sql.window import Window
from pyspark.sql.types import DateType
from pyspark.sql.functions import sum as spark_sum, avg as spark_avg, count as spark_count, countDistinct, trim

print("🥇 Gold Layer başlatılıyor...")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. FACT_BOOKINGS

# CELL ********************

df_silver = spark.read.table("silver_bookings")

# Filter: 1900 sentinel dates & NULL hotel_id
df_fact = df_silver.filter(
    (col("booking_date") > lit("1950-01-01").cast("date")) &
    col("hotel_id").isNotNull() &
    col("booking_id").isNotNull()
)

df_fact = df_fact.select(
    # Keys
    col("booking_id"),
    col("hotel_id"),
    col("customer_id"),
    
    # Date
    col("booking_date"),
    year("booking_date").alias("booking_year"),
    month("booking_date").alias("booking_month"),
    
    # Location
    col("city_clean").alias("city"),
    col("country"),
    
    # Hotel
    col("hotel_name"),
    col("hotel_type"),
    col("star_rating"),
    
    # Booking details
    col("room_type"),
    col("nights"),
    col("adults"),
    col("children"),
    col("booking_status"),
    col("source_system"),
    
    # Financial
    col("currency"),
    col("total_amount"),
    col("room_price"),
    coalesce(col("exchange_rate_to_eur"), lit(1.0)).alias("exchange_rate_to_eur"),
    
    # KPI: Revenue in EUR
    spark_round(
        col("total_amount") * coalesce(col("exchange_rate_to_eur"), lit(1.0)), 2
    ).alias("total_amount_eur"),
    
    # KPI: Avg nightly rate
    when(col("nights") > 0,
         spark_round(col("total_amount") / col("nights"), 2)
    ).otherwise(col("total_amount")).alias("avg_nightly_rate"),
    
    # KPI: Avg nightly rate EUR
    when(col("nights") > 0,
         spark_round(
             (col("total_amount") * coalesce(col("exchange_rate_to_eur"), lit(1.0))) / col("nights"), 2
         )
    ).otherwise(
         spark_round(col("total_amount") * coalesce(col("exchange_rate_to_eur"), lit(1.0)), 2)
    ).alias("avg_nightly_rate_eur"),
    
    # Weather
    col("temperature_c"),
    col("weather_code")
)

df_fact.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_fact_bookings")

count = df_fact.count()
print(f"✅ gold_fact_bookings: {count:,} rows")
df_fact.select("booking_id", "city", "currency", "total_amount", "total_amount_eur", "avg_nightly_rate_eur").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. DIM_CITY

# CELL ********************

df_city_dim = spark.read.table("silver_city_dim")

df_dim_city = df_city_dim.filter(
    col("city").isNotNull() & (col("city") != "Unknown")
).dropDuplicates(["city"]).select(
    row_number().over(Window.orderBy("city")).alias("city_key"),
    col("city"),
    col("country"),
    col("temperature_c"),
    col("weather_code"),
    coalesce(col("rate_eur"), lit(0)).alias("rate_eur"),
    coalesce(col("rate_usd"), lit(0)).alias("rate_usd"),
    coalesce(col("rate_gbp"), lit(0)).alias("rate_gbp"),
    coalesce(col("rate_jpy"), lit(0)).alias("rate_jpy"),
    coalesce(col("rate_try"), lit(0)).alias("rate_try"),
    coalesce(col("rate_aed"), lit(0)).alias("rate_aed"),
    coalesce(col("rate_cny"), lit(0)).alias("rate_cny")
)

df_dim_city.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_dim_city")

print(f"✅ gold_dim_city: {df_dim_city.count()} cities (unique)")
df_dim_city.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. DIM_HOTEL

# CELL ********************

df_silver = spark.read.table("silver_bookings")

w = Window.partitionBy("hotel_id").orderBy(col("booking_date").desc())

df_hotels = df_silver.filter(col("hotel_id").isNotNull()) \
    .withColumn("rn", row_number().over(w)) \
    .filter(col("rn") == 1) \
    .select(
        col("hotel_id"),
        col("hotel_name"),
        col("city_clean").alias("city"),
        col("country"),
        col("hotel_type"),
        col("star_rating"),
        col("latitude"),
        col("longitude"),
        when(col("star_rating") >= 4, lit("Premium"))
            .when(col("star_rating") >= 3, lit("Standard"))
            .otherwise(lit("Budget")).alias("hotel_tier")
    )

df_hotels.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_dim_hotel")

print(f"✅ gold_dim_hotel: {df_hotels.count()} hotels")
df_hotels.groupBy("hotel_tier").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. DIM_DATE

# CELL ********************

# Generate date spine: 2020-01-01 → 2026-12-31
df_dates = spark.sql("""
    SELECT explode(sequence(
        to_date('2020-01-01'),
        to_date('2026-12-31'),
        interval 1 day
    )) AS date_key
""")

df_dim_date = df_dates.select(
    col("date_key"),
    year("date_key").alias("year"),
    month("date_key").alias("month"),
    dayofmonth("date_key").alias("day"),
    quarter("date_key").alias("quarter"),
    date_format("date_key", "MMMM").alias("month_name"),
    date_format("date_key", "MMM").alias("month_short"),
    date_format("date_key", "EEEE").alias("day_name"),
    date_format("date_key", "EEE").alias("day_short"),
    weekofyear("date_key").alias("week_of_year"),
    dayofweek("date_key").alias("day_of_week"),
    dayofyear("date_key").alias("day_of_year"),
    when(dayofweek("date_key").isin(1, 7), lit(True))
        .otherwise(lit(False)).alias("is_weekend"),
    expr("concat(year(date_key), '-Q', quarter(date_key))").alias("year_quarter"),
    date_format("date_key", "yyyy-MM").alias("year_month")
)

df_dim_date.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_dim_date")

print(f"✅ gold_dim_date: {df_dim_date.count()} days (2020-2026)")
df_dim_date.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # KPI

# CELL ********************

df_fact = spark.read.table("gold_fact_bookings")
df_city = spark.read.table("gold_dim_city").dropDuplicates(["city"])

df_kpi = df_fact.groupBy(
    "city", "country", "booking_year", "booking_month", "hotel_type"
).agg(
    spark_count("*").alias("total_bookings"),
    spark_round(spark_sum("total_amount_eur"), 2).alias("total_revenue_eur"),
    spark_round(spark_avg("total_amount_eur"), 2).alias("avg_booking_value_eur"),
    spark_round(spark_avg("avg_nightly_rate_eur"), 2).alias("avg_nightly_rate_eur"),
    spark_round(spark_avg("nights"), 1).alias("avg_nights"),
    spark_sum(col("adults") + coalesce(col("children"), lit(0))).alias("total_guests"),
    spark_round(
        spark_sum(when(col("booking_status") == "Canceled", 1).otherwise(0)) * 100.0 / spark_count("*"), 1
    ).alias("cancellation_rate")
)

df_kpi_final = df_kpi.join(
    df_city.select("city", col("temperature_c").alias("city_temperature_c"), col("weather_code").alias("city_weather_code")),
    on="city", how="left"
)

df_kpi_final.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_kpi_revenue")

print(f"✅ gold_kpi_revenue: {df_kpi_final.count():,} rows")
df_kpi_final.orderBy(col("total_revenue_eur").desc()).show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Summary

# CELL ********************

print("\n" + "="*60)
print("🥇 GOLD LAYER COMPLETE")
print("="*60)

gold_tables = ["gold_fact_bookings", "gold_dim_city", "gold_dim_hotel", "gold_dim_date"]
for t in gold_tables:
    try:
        c = spark.read.table(t).count()
        print(f"  ✅ {t:<25} → {c:>10,} rows")
    except:
        print(f"  ❌ {t:<25} → NOT FOUND")

print("="*60)
print("\n🎯 Power BI'da Star Schema:")
print("   fact_bookings → dim_city  (city = city)")
print("   fact_bookings → dim_hotel (hotel_id = hotel_id)")
print("   fact_bookings → dim_date  (booking_date = date_key)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
