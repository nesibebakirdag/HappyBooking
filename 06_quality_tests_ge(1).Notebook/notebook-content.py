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
# META     },
# META     "environment": {
# META       "environmentId": "53e2ac77-9251-8735-4fc7-e25ce1360141",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Adım 6: Veri Kalite Testleri (Great Expectations)
# 
# **Amaç:** `silver_bookings` tablosunun kalitesini doğrulamak.
# 
# | Test | Açıklama |
# |------|----------|
# | Completeness | Kritik kolonlarda NULL yok |
# | Uniqueness | `booking_id` benzersiz |
# | Validity | Fiyatlar ≥ 0, tarihler geçerli |
# | Type Check | Kolom tipleri doğru |
# 
# **Not:** GX kurulumu için kernel restart gerekebilir.

# CELL ********************

%pip install great_expectations==0.18.21 --quiet

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

import great_expectations as ge
from great_expectations.dataset import SparkDFDataset
import pyspark.sql.functions as F

print(f"✅ Great Expectations version: {ge.__version__}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Veriyi Yükle

# CELL ********************

df_silver = spark.read.table("silver_bookings")
gx_df = SparkDFDataset(df_silver)

print(f"✅ silver_bookings yüklendi. Satır sayısı: {df_silver.count()}")
print(f"Kolonlar: {df_silver.columns}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Expectation'ları Tanımla

# CELL ********************

print("🧪 A. Completeness (Not Null) Testleri...")
gx_df.expect_column_values_to_not_be_null("booking_id")
gx_df.expect_column_values_to_not_be_null("customer_id")   # FIX: client_id → customer_id
gx_df.expect_column_values_to_not_be_null("hotel_id")
gx_df.expect_column_values_to_not_be_null("booking_date")

print("🧪 B. Uniqueness Testleri...")
gx_df.expect_column_values_to_be_unique("booking_id")

print("🧪 C. Validity (Fiyat ≥ 0) Testleri...")
gx_df.expect_column_values_to_be_between("total_amount", min_value=0)
gx_df.expect_column_values_to_be_between("room_price", min_value=0)

print("🧪 D. Type Testleri...")
gx_df.expect_column_values_to_be_of_type("booking_id", "StringType")
gx_df.expect_column_values_to_be_of_type("total_amount", "DoubleType")
gx_df.expect_column_values_to_be_of_type("customer_id", "StringType")

print("🧪 E. Domain Testleri...")
gx_df.expect_column_values_to_be_in_set(
    "currency",
    ["EUR", "USD", "GBP", "JPY", "TRY", "AED", "CNY"]
)

print("✅ Tüm expectation'lar tanımlandı.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Validasyon Çalıştır & Rapor

# CELL ********************

print("📊 Full Validation Suite çalışıyor...")
results = gx_df.validate()

# Özet
total_tests  = len(results["results"])
passed_tests = sum(1 for r in results["results"] if r["success"])
failed_tests = total_tests - passed_tests

print(f"\n{'='*50}")
print(f"📋 VALIDATION REPORT — silver_bookings")
print(f"{'='*50}")
print(f"  Toplam Test  : {total_tests}")
print(f"  ✅ Geçti     : {passed_tests}")
print(f"  ❌ Kaldı     : {failed_tests}")
print(f"{'='*50}")

if results["success"]:
    print("🎉 SUCCESS! Tüm kalite kontrolleri geçti.")
else:
    print("⚠️ Bazı testler başarısız:")
    for res in results["results"]:
        status = "✅" if res["success"] else "❌"
        exp_type = res["expectation_config"]["expectation_type"]
        col_name = res["expectation_config"]["kwargs"].get("column", "")
        print(f"  {status} {exp_type}({col_name})")
        if not res["success"] and "result" in res:
            print(f"       → {res['result']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Native PySpark Kalite Özeti
# 
# GX kurulumu başarısız olursa bu cell'i çalıştır.

# CELL ********************

from pyspark.sql.functions import col, count, when, isnan, isnull

df = spark.read.table("silver_bookings")
total = df.count()

checks = [
    ("booking_id NULL",    df.filter(col("booking_id").isNull()).count()),
    ("customer_id NULL",   df.filter(col("customer_id").isNull()).count()),
    ("hotel_id NULL",      df.filter(col("hotel_id").isNull()).count()),
    ("booking_date NULL",  df.filter(col("booking_date").isNull()).count()),
    ("total_amount < 0",   df.filter(col("total_amount") < 0).count()),
    ("city_clean NULL",    df.filter(col("city_clean").isNull()).count()),
    ("duplicate booking",  total - df.select("booking_id").distinct().count()),
]

print(f"\n{'='*55}")
print(f"{'TEST':<30} {'SORUNLU':>10} {'DURUM':>10}")
print(f"{'='*55}")
for name, count_val in checks:
    status = "✅ OK" if count_val == 0 else f"❌ {count_val:,}"
    print(f"{name:<30} {count_val:>10,} {status:>10}")
print(f"{'='*55}")
print(f"Toplam satır: {total:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
