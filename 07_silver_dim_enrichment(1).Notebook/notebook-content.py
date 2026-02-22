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

# # 07 - Silver City Dimension: Full Enrichment
# 
# **Sonuç Tablo Yapısı:**
# 
# | Kolon | Açıklama |
# |-------|----------|
# | `city`, `country` | Şehir bilgisi |
# | `temperature_c`, `weather_code` | Hava durumu |
# | `rate_eur` | Her zaman 1.0 (base) |
# | `rate_usd`, `rate_gbp`, `rate_jpy`, `rate_try`, `rate_aed`, `rate_cny` | Tüm kurlar |
# 
# **Neden bu yapı?**
# - `silver_bookings`'deki para birimi ne olursa olsun dim her zaman tam.
# - Power BI'da dinamik para birimi filtresi yapılabilir.
# - Gold Layer'da `total_amount * rate_xxx` ile istenen kurda KPI üretilir.

# CELL ********************

from pyspark.sql.functions import col, upper, trim, initcap, lit, when, coalesce, regexp_replace, max as spark_max

DIM_TABLE = "silver_city_dim"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Load Sources

# CELL ********************

df_bookings = spark.read.table("silver_bookings")
df_weather  = spark.read.table("bronze_weather")
df_rates    = spark.read.table("bronze_exchange_rates")

print(f"✅ Bookings: {df_bookings.count()} | Weather: {df_weather.count()} | Rates: {df_rates.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Extract Unique Cities

# CELL ********************

# Determine correct city column name
city_col = "city_clean" if "city_clean" in df_bookings.columns else "city"

df_cities = df_bookings.select(
    initcap(trim(regexp_replace(col(city_col), r'\.+$', ''))).alias("city"),
    initcap(trim(col("country"))).alias("country")
).distinct().filter(
    col("city").isNotNull() &
    (col("city") != "Unknown") &
    (col("city") != "")
)

print(f"🌆 Unique Cities: {df_cities.count()}")
df_cities.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Add Weather Data

# CELL ********************

# Normalize weather city names
df_w = df_weather.select(
    initcap(trim(col("city"))).alias("w_city"),
    col("temperature_c"),
    col("weather_code")
).dropDuplicates(["w_city"])

# JOIN cities with weather
df_with_weather = df_cities.join(
    df_w,
    df_cities.city == df_w.w_city,
    how="left"
).drop("w_city")

matched = df_with_weather.filter(col("temperature_c").isNotNull()).count()
print(f"🌡️ Weather matched: {matched} / {df_cities.count()} cities")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Pivot All Exchange Rates as Separate Columns
# Her şehir için **tüm döviz kurları** ayrı kolonlarda. Booking currency'den bağımsız.

# CELL ********************

# Normalize rates table
df_r = df_rates.select(
    upper(trim(col("target_currency"))).alias("currency"),
    col("rate").alias("rate_value")
).dropDuplicates(["currency"])

# Show available currencies
print("💱 Available exchange rates:")
df_r.show()

# Manually pull each rate (robust approach, no dynamic pivot needed)
def get_rate(currency_code):
    """Returns the exchange rate for a currency, or None if not found."""
    row = df_r.filter(col("currency") == currency_code).first()
    return float(row["rate_value"]) if row else None

rate_eur = 1.0
rate_usd = get_rate("USD")
rate_gbp = get_rate("GBP")
rate_jpy = get_rate("JPY")
rate_try = get_rate("TRY")
rate_aed = get_rate("AED")
rate_cny = get_rate("CNY")

print(f"EUR=1.0 | USD={rate_usd} | GBP={rate_gbp} | JPY={rate_jpy} | TRY={rate_try} | AED={rate_aed} | CNY={rate_cny}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Build Final Dim Table

# CELL ********************

# Add all rates as columns to every city row
df_dim = df_with_weather

df_dim = df_dim.withColumn("rate_eur", lit(rate_eur))
df_dim = df_dim.withColumn("rate_usd", lit(rate_usd))
df_dim = df_dim.withColumn("rate_gbp", lit(rate_gbp))
df_dim = df_dim.withColumn("rate_jpy", lit(rate_jpy))
df_dim = df_dim.withColumn("rate_try", lit(rate_try))
df_dim = df_dim.withColumn("rate_aed", lit(rate_aed))
df_dim = df_dim.withColumn("rate_cny", lit(rate_cny))

# Save
df_dim.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(DIM_TABLE)

print(f"✅ {DIM_TABLE} saved! ({df_dim.count()} cities)")
df_dim.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
