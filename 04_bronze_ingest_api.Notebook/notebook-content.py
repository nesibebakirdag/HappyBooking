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

# # Bronze Layer: API Ingestion (Data-Driven Enrichment)
# 
# **Amaç:**
# 1. **Enrichment:** Silver'daki temizlenmiş şehirler (`city_clean`) için gerçek hava durumu çek.
# 2. **Reference Data:** Weather ve Exchange Rate tabloları Dim Join için kullanılır.
# 
# **⚠️ Çalıştırma Sırası:**
# ```
# 05_silver_transformations  →  04_bronze_ingest_api  →  07_silver_dim_enrichment
# ```
# Silver önce çalıştırılır çünkü temiz `city_clean` isimleri oradan okunur.

# CELL ********************

import requests
import json
import os
import datetime
from pyspark.sql.functions import col, to_timestamp, initcap, trim, lit, regexp_replace

RAW_WEATHER_PATH  = "/lakehouse/default/Files/raw_api/weather"
RAW_EXCHANGE_PATH = "/lakehouse/default/Files/raw_api/exchange"
WEATHER_TABLE     = "bronze_weather"
EXCHANGE_TABLE    = "bronze_exchange_rates"

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Hava Durumu (Silver city_clean → Geocoding → Weather API)

# CELL ********************

print("🌦️ Fetching Weather Data (Data-Driven Enrichment)...")
try:
    unique_cities = []

    # PRIMARY SOURCE: silver_bookings.city_clean (temizlenmiş isimler, API için ideal)
    try:
        df_silver = spark.read.table("silver_bookings")
        city_col = "city_clean" if "city_clean" in df_silver.columns else "city"
        cities_silver = [
            row[city_col] for row in
            df_silver.select(city_col).distinct()
            .filter(col(city_col).isNotNull() & (col(city_col) != "Unknown") & (col(city_col) != ""))
            .collect()
        ]
        unique_cities.extend(cities_silver)
        print(f"✅ Silver'dan {len(cities_silver)} temiz şehir okundu.")
    except Exception as e:
        print(f"⚠️ Silver okuma hatası: {e}")

    # FALLBACK: Bronze'dan oku (Silver yoksa)
    if not unique_cities:
        print("⚠️ Silver bulunamadı, Bronze'dan okunuyor (kirli isimler olabilir)...")
        try:
            df_batch = spark.read.table("bronze_hotel_batch")
            cities_batch = [row.city for row in df_batch.select("city").distinct().limit(500).collect() if row.city]
            unique_cities.extend(cities_batch)
        except:
            pass

    # Deduplicate
    unique_cities = list(set([c.strip() for c in unique_cities if c and c.strip()]))

    if not unique_cities:
        unique_cities = ["Amsterdam", "Berlin", "Paris", "London", "Istanbul"]

    print(f"📊 Toplam {len(unique_cities)} benzersiz şehir işlenecek.")
    print(f"Örnek: {unique_cities[:10]}...")

    ensure_dir(RAW_WEATHER_PATH)
    weather_records = []
    session = requests.Session()
    count = 0
    failed = 0

    for city in unique_cities:
        try:
            # A. Geocoding
            geo_url = "https://geocoding-api.open-meteo.com/v1/search"
            geo_params = {"name": city, "count": 1, "language": "en", "format": "json"}
            geo_resp = session.get(geo_url, params=geo_params, timeout=10)
            geo_data = geo_resp.json()

            if not geo_data.get("results"):
                failed += 1
                continue

            lat     = geo_data["results"][0]["latitude"]
            lon     = geo_data["results"][0]["longitude"]
            country = geo_data["results"][0]["country"]

            # B. Weather
            weather_url = "https://api.open-meteo.com/v1/forecast"
            w_params = {
                "latitude": lat, "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m,weather_code",
                "timezone": "auto"
            }
            w_resp = session.get(weather_url, params=w_params, timeout=10)
            w_data = w_resp.json()

            # C. Save Raw JSON
            timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_city = city.replace(" ", "_").replace("/", "_")
            with open(f"{RAW_WEATHER_PATH}/weather_{safe_city}_{timestamp_str}.json", "w") as f:
                json.dump(w_data, f)

            # D. Normalize
            current = w_data.get("current", {})
            record = {
                "city": city,
                "country_api": country,
                "latitude": lat,
                "longitude": lon,
                "temperature_c": float(current.get("temperature_2m")),
                "humidity": int(current.get("relative_humidity_2m")),
                "weather_code": int(current.get("weather_code")),
                "recorded_at": current.get("time"),
                "source": "open-meteo",
                "ingestion_time": datetime.datetime.now().isoformat()
            }
            weather_records.append(record)
            count += 1
            if count % 20 == 0:
                print(f"✅ {count} şehir işlendi ({failed} başarısız)...")

        except Exception as inner_e:
            failed += 1

    if weather_records:
        df_weather = spark.createDataFrame(weather_records)
        df_weather_normalized = df_weather \
            .withColumn("city", initcap(trim(col("city")))) \
            .withColumn("recorded_at", to_timestamp(col("recorded_at"))) \
            .withColumn("ingestion_time", to_timestamp(col("ingestion_time")))

        df_weather_normalized.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true").saveAsTable(WEATHER_TABLE)
        print(f"\n✅ {len(weather_records)} şehir kaydedildi → {WEATHER_TABLE}  ({failed} şehir bulunamadı)")
        df_weather_normalized.select("city", "country_api", "temperature_c", "weather_code").show(10, truncate=False)
    else:
        print("⚠️ Hiçbir hava durumu kaydı bulunamadı.")

except Exception as e:
    print(f"❌ Kritik Hata: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Döviz Kurları (Exchange Rates)

# CELL ********************

CURRENCY_URL = "https://open.er-api.com/v6/latest/EUR"
print("💰 Fetching Exchange Rates...")
try:
    response = requests.get(CURRENCY_URL, timeout=15)
    response.raise_for_status()
    data = response.json()

    ensure_dir(RAW_EXCHANGE_PATH)
    timestamp_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"{RAW_EXCHANGE_PATH}/exchange_{timestamp_str}.json", "w") as f:
        json.dump(data, f)

    rates = data.get("rates", {})
    base  = data.get("base_code", "EUR")
    date_str = data.get("time_last_update_utc")

    target_currencies = ["USD", "GBP", "JPY", "TRY", "AED", "CNY"]
    records = []
    for curr in target_currencies:
        if curr in rates:
            records.append({
                "base_currency": base,
                "target_currency": curr,
                "rate": float(rates[curr]),
                "timestamp": date_str,
                "source": "exchangerate-api",
                "ingestion_time": datetime.datetime.now().isoformat()
            })

    df_rates = spark.createDataFrame(records)
    df_rates_normalized = df_rates \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("ingestion_time", to_timestamp(col("ingestion_time")))

    df_rates_normalized.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").saveAsTable(EXCHANGE_TABLE)
    print(f"✅ Exchange Rates Saved → {EXCHANGE_TABLE}")
    df_rates_normalized.show()

except Exception as e:
    print(f"❌ Exchange Rate API Error: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
