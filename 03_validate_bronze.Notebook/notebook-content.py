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

# # Streaming Data Validation
# Bu notebook, Eventstream üzerinden Lakehouse'a akan verilerin sayısını doğrulamak için kullanılır.

# CELL ********************

# Tablo adını kontrol edelim (önceki adımlarda bronze_hotel_stream olarak belirlemiştik)
table_name = "bronze_hotel_stream"

# SQL ile sayım yapalım
df_count = spark.sql(f"SELECT COUNT(*) as total_rows FROM {table_name}")
df_count.show()

# Son 5 kaydı görelim
df_sample = spark.sql(f"SELECT * FROM {table_name} ORDER BY event_time DESC LIMIT 100")
df_sample.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
