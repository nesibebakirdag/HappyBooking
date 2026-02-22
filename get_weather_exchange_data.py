import requests
import pandas as pd
import os
import json
import datetime
import glob

# --- KONFİGÜRASYON ---
OUTPUT_DIR = "data/api_data"
RAW_DATA_PATTERN = "data/*.csv" 

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# API URL'leri
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
CURRENCY_URL = "https://open.er-api.com/v6/latest/EUR"

def get_unique_top_cities(limit=5):
    """Veriden en çok tekrar eden şehirleri bulur."""
    try:
        files = glob.glob(RAW_DATA_PATTERN)
        if not files:
            print("⚠️ Hiç veri dosyası bulunamadı, varsayılan şehirler kullanılacak.")
            return ["Amsterdam", "Berlin", "London", "Paris", "Istanbul"]
            
        dfs = []
        for f in files:
            try:
                # Sadece city kolonunu oku, performansı koru
                dfs.append(pd.read_csv(f, usecols=["city"]))
            except Exception:
                pass # Hatalı dosyayı atla
        
        if not dfs: return ["Amsterdam"]
        
        df = pd.concat(dfs, ignore_index=True)
        # En çok geçen 'limit' kadar şehri al
        top_cities = df['city'].value_counts().head(limit).index.tolist()
        print(f"📊 Veriden bulunan top {limit} şehir: {top_cities}")
        return top_cities
    except Exception as e:
        print(f"⚠️ Şehir bulma hatası: {e}. Varsayılanlar kullanılıyor.")
        return ["Amsterdam", "Berlin"] # Fallback

def get_real_coordinates(city_name):
    """Kirli verideki hatalı koordinatlar yerine API'den gerçeğini sorar."""
    try:
        params = {"name": city_name, "count": 1, "language": "en", "format": "json"}
        resp = requests.get(GEOCODING_URL, params=params)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if results:
            return results[0]["latitude"], results[0]["longitude"], results[0]["country"]
    except Exception as e:
        print(f"❌ Geocoding hatası ({city_name}): {e}")
    return None, None, None

def fetch_weather_enrichment():
    print(f"\n�️  Hava Durumu Zenginleştirme Başlıyor (Data-Driven)...")
    
    cities = get_unique_top_cities(limit=10) # İlk 10 şehir için hava durumu al
    weather_records = []
    
    for city in cities:
        # 1. Gerçek Koordinatları Bul (Data Cleaning on the fly!)
        lat, lon, country = get_real_coordinates(city)
        
        if lat is None:
            print(f"⏩ {city} için koordinat bulunamadı, geçiliyor.")
            continue
            
        # 2. O Koordinat için Hava Durumu Çek
        try:
            params = {
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m,weather_code",
                "timezone": "auto"
            }
            resp = requests.get(WEATHER_URL, params=params)
            resp.raise_for_status()
            data = resp.json()
            current = data.get("current", {})
            
            record = {
                "city": city, # Bizim verideki şehir ismi
                "country_api": country, # API'den gelen gerçek ülke
                "real_latitude": lat, # Düzeltilmiş koordinat
                "real_longitude": lon,
                "temperature_c": current.get("temperature_2m"),
                "humidity": current.get("relative_humidity_2m"),
                "weather_code": current.get("weather_code"),
                "recorded_at": current.get("time"),
                "source": "open-meteo",
                "ingestion_time": datetime.datetime.now().isoformat()
            }
            weather_records.append(record)
            print(f"✅ {city} ({country}): {record['temperature_c']}°C")
            
        except Exception as e:
            print(f"❌ Weather hatası ({city}): {e}")

    # Kaydet
    if weather_records:
        df = pd.DataFrame(weather_records)
        csv_path = f"{OUTPUT_DIR}/weather_enriched.csv"
        json_path = f"{OUTPUT_DIR}/weather_enriched.json"
        
        df.to_csv(csv_path, index=False)
        df.to_json(json_path, orient="records", lines=True)
        print(f"💾 Hava durumu verisi kaydedildi: {len(df)} şehir.")
        return df
    return None

def fetch_exchange_rates():
    print(f"\n💰 Döviz Kurları Referans Verisi Çekiliyor...")
    # Dönüşüm Senaryosu: "Keşke elimizdeki tüm tutarları USD veya EUR cinsinden görebilsek"
    try:
        response = requests.get(CURRENCY_URL)
        response.raise_for_status()
        data = response.json()
        
        rates = data.get("rates", {})
        base = data.get("base_code", "EUR")
        targets = ["USD", "GBP", "JPY", "TRY", "AED", "CNY"]
        
        records = []
        for curr in targets:
            if curr in rates:
                records.append({
                    "base_currency": base,
                    "target_currency": curr,
                    "rate": rates[curr],
                    "timestamp": datetime.datetime.now().isoformat(),
                    "source": "open-er-api"
                })
                
        df = pd.DataFrame(records)
        csv_path = f"{OUTPUT_DIR}/exchange_rates_ref.csv"
        df.to_csv(csv_path, index=False)
        print(f"✅ Kur verisi alındı. 1 EUR = {rates.get('TRY', '?')} TRY")
        return df
        
    except Exception as e:
        print(f"❌ Döviz hatası: {e}")
        return None

if __name__ == "__main__":
    print("--- 🚀 PROFESYONEL API ENTEGRASYONU ---")
    print("Amaç: Kirli veriyi (hatalı koordinatları) düzeltmek ve dış veriyle zenginleştirmek.")
    fetch_weather_enrichment()
    fetch_exchange_rates()
    print("--- Tamamlandı ---")
