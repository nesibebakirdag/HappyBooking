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

# # Silver Layer: Advanced Data Transformation & Enrichment
# 
# **Pipeline Sırası:**
# ```
# 05_silver_transformations  →  04_bronze_ingest_api  →  07_silver_dim_enrichment
# ```

# CELL ********************

from pyspark.sql.functions import (col, coalesce, to_timestamp, lower, trim, initcap, upper,
                                    regexp_replace, when, round, abs, create_map, lit, length,
                                    to_date, date_format)
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType
import pyspark.sql.functions as F
from itertools import chain

SILVER_TABLE = "silver_bookings"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Load & Union

# CELL ********************

df_batch = spark.read.table("bronze_hotel_batch")
df_stream = spark.read.table("bronze_hotel_stream")

df_raw = df_batch.unionByName(df_stream, allowMissingColumns=True)
print(f"✅ Batch: {df_batch.count()} | Stream: {df_stream.count()} | Union: {df_raw.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Hotel Lookup: Stream'deki NULL city/country'yi doldur
# 
# Stream'den gelen satırlar yalnızca `hotel_id` içerir. Batch'ten hotel dim oluşturup join ile dolduruyoruz.

# CELL ********************

# Step 1: Batch'ten hotel dimension oluştur
hotel_meta_cols = ["hotel_name", "city", "country", "star_rating", "hotel_type", "latitude", "longitude"]
available_meta = [c for c in hotel_meta_cols if c in df_batch.columns]

print(f"📋 Batch kolonları: {df_batch.columns}")
print(f"📋 Available meta: {available_meta}")

df_dim = df_batch.select(
    regexp_replace(col("hotel_id"), r"[^0-9]", "").alias("dim_id"),
    *[col(c).alias(f"dim_{c}") for c in available_meta]
).dropDuplicates(["dim_id"])

print(f"📋 Dim boyutu: {df_dim.count()} unique hotel")
df_dim.show(3, truncate=False)

# Step 2: df_raw'daki hotel_id'yi aynı formata getir
df_with_clean_id = df_raw.withColumn(
    "hotel_id_num",
    regexp_replace(col("hotel_id"), r"[^0-9]", "")
)

# Step 3: LEFT JOIN
df_joined = df_with_clean_id.join(
    df_dim,
    df_with_clean_id.hotel_id_num == df_dim.dim_id,
    how="left"
)

# Step 4: NULL + boş string + "Unknown" → dim'den doldur
df_filled = df_joined
for c in available_meta:
    if c in df_filled.columns:
        df_filled = df_filled.withColumn(c,
            when(
                col(c).isNull() | (trim(col(c)) == "") | (col(c) == "Unknown"),
                col(f"dim_{c}")
            ).otherwise(col(c))
        )
    else:
        df_filled = df_filled.withColumn(c, col(f"dim_{c}"))

# Step 5: Geçici kolonları temizle
drop_cols = ["hotel_id_num", "dim_id"] + [f"dim_{c}" for c in available_meta]
df_enriched_stream = df_filled.drop(*[c for c in drop_cols if c in df_filled.columns])

# ===== DİAGNOSTİK =====
print("\n📊 HOTEL_NAME Kontrol:")
print(f"  ÖNCE  → NULL: {df_raw.filter(col('hotel_name').isNull()).count()}, "
      f"Boş: {df_raw.filter(trim(col('hotel_name')) == '').count()}")
print(f"  SONRA → NULL: {df_enriched_stream.filter(col('hotel_name').isNull()).count()}, "
      f"Boş: {df_enriched_stream.filter(trim(col('hotel_name')) == '').count()}")
df_enriched_stream.select("hotel_id", "hotel_name", "hotel_type", "city").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Standardize & Deduplicate

# CELL ********************

df_cleaned = df_enriched_stream.toDF(
    *[c.strip().lower().replace(" ", "_") for c in df_enriched_stream.columns]
)

if "total_amount" in df_cleaned.columns and "total_price" in df_cleaned.columns:
    df_cleaned = df_cleaned.drop("total_price")
elif "total_price" in df_cleaned.columns:
    df_cleaned = df_cleaned.withColumnRenamed("total_price", "total_amount")

if "total_amount" in df_cleaned.columns and "paid_amount" in df_cleaned.columns:
    df_cleaned = df_cleaned.drop("paid_amount")
elif "paid_amount" in df_cleaned.columns:
    df_cleaned = df_cleaned.withColumnRenamed("paid_amount", "total_amount")

# Remove exact duplicate column names (if any remain)
seen = set()
unique_cols = [c for c in df_cleaned.columns if not (c in seen or seen.add(c))]
df_cleaned = df_cleaned.select(unique_cols)

window_spec = Window.partitionBy("booking_id").orderBy(col("ingestion_time").desc())
df_dedup = df_cleaned.withColumn("rn", F.row_number().over(window_spec)) \
    .filter(col("rn") == 1).drop("rn")

print(f"📉 Dedup complete. Rows: {df_dedup.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Type Casting

# CELL ********************

if "total_amount" not in df_dedup.columns:
    df_dedup = df_dedup.withColumn("total_amount", lit(None).cast("double"))

if "currency" not in df_dedup.columns:
    df_dedup = df_dedup.withColumn("currency", lit("EUR"))

df_typed = df_dedup \
    .withColumn("booking_date", to_timestamp(col("booking_date"))) \
    .withColumn("total_amount", abs(col("total_amount").cast("double"))) \
    .withColumn("room_price",   abs(col("room_price").cast("double"))) \
    .withColumn("nights",       col("nights").cast("integer"))

df_valid_dates = df_typed.filter(col("booking_date").isNotNull())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Enrichment (Weather + Exchange Rate JOIN)

# CELL ********************

df_weather = spark.read.table("bronze_weather")
df_rates   = spark.read.table("bronze_exchange_rates")

df_w = df_weather.select(
    initcap(trim(col("city"))).alias("w_city"),
    col("temperature_c"),
    col("weather_code")
)

df_r = df_rates.select(
    upper(trim(col("target_currency"))).alias("r_currency"),
    col("rate").alias("exchange_rate_to_eur")
)

# city_clean: trailing nokta/boşluk temizle
df_main = df_valid_dates.withColumn("city_clean",
    initcap(trim(regexp_replace(col("city"), r'[.\s]+$', '')))
)

df_enriched = df_main \
    .join(df_w, df_main.city_clean == df_w.w_city, "left") \
    .join(df_r, upper(trim(df_main.currency)) == df_r.r_currency, "left") \
    .drop("w_city", "r_currency")

# EUR base currency = 1.0
df_enriched = df_enriched.withColumn("exchange_rate_to_eur",
    when(upper(trim(col("currency"))) == "EUR", lit(1.0))
    .otherwise(col("exchange_rate_to_eur"))
)

print("🌍 Enrichment Complete.")
df_enriched.select("city", "city_clean", "currency", "temperature_c", "exchange_rate_to_eur").show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Smart Imputation

# CELL ********************

df_smart = df_enriched \
    .withColumn("room_price",
                when(col("room_price").isNull() & col("total_amount").isNotNull() & (col("nights") > 0),
                     round(col("total_amount") / col("nights"), 2))
                .otherwise(col("room_price"))) \
    .withColumn("total_amount",
                when(col("total_amount").isNull() & col("room_price").isNotNull(),
                     round(col("room_price") * col("nights"), 2))
                .otherwise(col("total_amount")))

print("🧠 Smart Imputation Applied.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Universal Cleaning

# CELL ********************

df_clean = df_smart

# Hotel metadata kolonlarını generic temizlemeden KORU
skip_cols = {"hotel_name", "hotel_type"}

all_cols = df_clean.columns

numeric_keywords = ["price", "amount", "cost", "tax", "rate", "rating", "votes", "score",
                    "rooms", "booked", "nights", "adults", "children", "infants", "capacity",
                    "latitude", "longitude", "discount", "fee", "coord"]
numeric_candidate_cols = [c for c in all_cols if any(k in c.lower() for k in numeric_keywords)]

date_keywords = ["date", "_at", "time", "day"]
date_candidate_cols = [c for c in all_cols if any(k in c.lower() for k in date_keywords)]

exclude_list = numeric_candidate_cols + date_candidate_cols
string_candidate_cols = [c for c in all_cols if c not in exclude_list and c not in skip_cols]

garbage_list = ["?", "???", "-", "--", "___", "null", "NULL", "Null", "NaN", " "]

for c in string_candidate_cols:
    df_clean = df_clean.withColumn(c, trim(col(c)))
    is_phone = any(k in c.lower() for k in ["phone", "contact", "mobile", "tel", "fax"])
    if is_phone:
        df_clean = df_clean.withColumn(c, regexp_replace(col(c), "[^0-9+]", ""))
        df_clean = df_clean.withColumn(c,
            when((length(col(c)) < 5) | col(c).contains("0000000"), lit("Unknown")).otherwise(col(c)))
    else:
        df_clean = df_clean.withColumn(c, regexp_replace(col(c), "^[!@#\\$%^&*()_+\\-=]+|[!@#\\$%^&*()_+\\-=]+$", ""))
    df_clean = df_clean.withColumn(c,
        when(col(c).isin(garbage_list) | (col(c) == ""), lit("Unknown")).otherwise(col(c)))
    # Para birimi UPPER, diğerleri Title Case
    if "currency" in c.lower():
        df_clean = df_clean.withColumn(c, upper(col(c)))
    else:
        df_clean = df_clean.withColumn(c, initcap(col(c)))

number_map = {"One": "1", "Two": "2", "Three": "3", "Four": "4", "Five": "5", "Ten": "10"}
mapping_expr = create_map([lit(x) for x in chain(*number_map.items())])

for c in numeric_candidate_cols:
    if c in df_clean.columns:
        if dict(df_clean.dtypes)[c] == "string":
            df_clean = df_clean.withColumn(c, F.coalesce(mapping_expr[col(c)], col(c)))
        type_to_cast = IntegerType() if any(x in c.lower() for x in ["rooms", "booked", "adults", "rating"]) else DoubleType()
        df_clean = df_clean.withColumn(c, col(c).cast(type_to_cast))
        is_coord = any(k in c.lower() for k in ["latitude", "longitude", "coord", "geo"])
        if not is_coord:
            df_clean = df_clean.withColumn(c, abs(col(c)))
            df_clean = df_clean.withColumn(c, F.coalesce(col(c), lit(0)))
        else:
            limit = 90 if "latitude" in c.lower() else 180
            df_clean = df_clean.withColumn(c,
                when((col(c) < -limit) | (col(c) > limit) | (col(c) == 0), None).otherwise(col(c)))

time_cols_to_check = []
for c in date_candidate_cols:
    if c in df_clean.columns:
        df_clean = df_clean.withColumn(f"{c}_ts", to_timestamp(col(c)))
        safe_ts = F.coalesce(col(f"{c}_ts"), to_timestamp(lit("1900-01-01 00:00:00")))
        df_clean = df_clean.withColumn(c, to_date(safe_ts))
        time_col = f"{c}_time"
        df_clean = df_clean.withColumn(time_col, date_format(safe_ts, "HH:mm:ss"))
        df_clean = df_clean.drop(f"{c}_ts")
        time_cols_to_check.append(time_col)

for t_col in time_cols_to_check:
    if df_clean.filter(col(t_col) != "00:00:00").limit(1).count() == 0:
        df_clean = df_clean.drop(t_col)

# Hotel metadata: sadece NULL/boş → Unknown, geri kalan dokunma
for c in ["hotel_name", "hotel_type"]:
    if c in df_clean.columns:
        df_clean = df_clean.withColumn(c,
            when(col(c).isNull() | (trim(col(c)) == ""), lit("Unknown"))
            .otherwise(trim(col(c))))
            
print("✨ Universal Cleaning Applied.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Final Write

# CELL ********************

df_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

total       = df_clean.count()
city_filled = df_clean.filter(col("city_clean").isNotNull() & (col("city_clean") != "Unknown")).count()
weather_ok  = df_clean.filter(col("temperature_c").isNotNull()).count()

print(f"🚀 silver_bookings yazıldı!")
print(f"   Toplam satır : {total}")
print(f"   City dolu    : {city_filled} ({int(city_filled/total*100)}%)")  # int() kullan
print(f"   Weather dolu : {weather_ok} ({int(weather_ok/total*100)}%)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
