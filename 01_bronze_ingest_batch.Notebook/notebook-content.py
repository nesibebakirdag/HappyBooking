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

# # Adım 1: Toplu Veri Yükleme (Bronze Layer)
# 
# Bu notebook, 'hotel_raw_batch.csv' dosyasını okuyup Fabric Lakehouse içerisine Delta tablosu olarak kaydeder.

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

# Otomatik Spark Oturumu (Fabric)
spark = SparkSession.builder.getOrCreate()

# Dosya Yolu: Files/data/hotel_raw_batch.csv
# (Oncesinde 'booking_dirty.csv'den uretilen batch dosyasini buraya yuklemelisiniz)
SOURCE_PATH = "Files/data/hotel_raw_batch.csv" 
BRONZE_TABLE_NAME = "bronze_hotel_batch"

print(f"Okunuyor: {SOURCE_PATH}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Dosyayı Oku
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(SOURCE_PATH)

# 2. Metadata Ekle
df_transformed = df.withColumn("ingestion_time", current_timestamp()) \
                   .withColumn("source_file", input_file_name())

# 3. Bronze Katmanına Yaz (Delta Table)
print(f"Tabloya yazılıyor: {BRONZE_TABLE_NAME}")

# HATA DUZELTME: overwriteSchema=true ile eski tablo semasini zorla degistiriyoruz
df_transformed.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(BRONZE_TABLE_NAME)

print("İşlem Başarılı! 'Tables' bölümünü yenileyerek tabloyu görebilirsiniz.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
