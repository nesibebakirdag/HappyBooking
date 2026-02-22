import pandas as pd
import time
import json
import os
import datetime
import random
import uuid
from azure.eventhub import EventHubProducerClient, EventData

# --- KONFİGÜRASYON ---
DATA_FILE = "data/hotel_raw_stream.csv" 
# Kullanıcıdan alacağımız Connection String (Environment variable olarak gelecek)
# --- CONNECTION STRING ---
# Buraya Event Hub Connection String'inizi yapıştırın (Tırnaklar içine)
CONNECTION_STRING = os.environ.get("EVENTHUB_CONNECTION_STRING", "")  # Set via environment variable
# Opsiyonel: Eğer connection string içinde EntityPath yoksa buraya Event Hub ismini yazın
EVENTHUB_NAME = os.environ.get("EVENTHUB_NAME", "") 

DELAY_SECONDS = 0.5 

def generate_booking(hotel):
    """Generates a random booking transaction based on a hotel."""
    return {
        # --- Hotel Metadata ---
        "hotel_id":    hotel.get("hotel_id"),
        "hotel_name":  hotel.get("hotel_name"),
        "city":        hotel.get("city"),
        "country":     hotel.get("country"),
        "star_rating": hotel.get("star_rating"),
        "hotel_type":  hotel.get("hotel_type"),
        "latitude":    hotel.get("latitude"),
        "longitude":   hotel.get("longitude"),
        # --- Booking Transaction ---
        "booking_id":  f"BKG_{uuid.uuid4().hex[:8].upper()}",
        "customer_id": random.randint(1000, 9999),
        "booking_date": datetime.datetime.now().isoformat(),
        "room_type":   random.choice(["Single", "Double", "Suite", "Deluxe"]),
        "nights":      random.randint(1, 14),
        "adults":      random.randint(1, 4),
        "total_amount": round(random.uniform(50, 2000), 2),
        "room_price":  round(random.uniform(30, 500), 2),
        "currency":    random.choice(["EUR", "USD", "GBP", "JPY", "TRY", "AED", "CNY"]),
        "booking_status": random.choice(["Confirmed", "Pending", "Cancelled"]),
        "source_system": random.choice(["MobileApp", "Web", "Agency"]),
        "ingestion_time": datetime.datetime.now().isoformat()
    }

def run_producer():
    print("--- Akış Simülatörü Başlatılıyor (Booking Generator Mode) ---")
    
    if not os.path.exists(DATA_FILE):
        print(f"HATA: Veri dosyası bulunamadı: {DATA_FILE}")
        return

    # Load Hotels into memory
    try:
        df_hotels = pd.read_csv(DATA_FILE)
        hotels_list = df_hotels.to_dict(orient="records")
        print(f"✅ {len(hotels_list)} Otel Yüklendi. Rastgele rezervasyon üretilecek.")
    except Exception as e:
        print(f"❌ Veri okuma hatası: {e}")
        return

    client = None
    if CONNECTION_STRING:
        try:
            # Client oluştur
            if EVENTHUB_NAME:
                client = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STRING, eventhub_name=EVENTHUB_NAME)
            else:
                client = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STRING)
            print("✅ Event Hub bağlantısı kuruldu.")
        except Exception as e:
            print(f"❌ Bağlantı hatası: {e}")
            print("Connection String'i kontrol edin.")
            return
    else:
        print("⚠️  UYARI: EVENTHUB_CONNECTION_STRING bulunamadı. Test modu (Ekrana yazılır).")

    print("Veri gönderimi başlıyor...\n")

    try:
        while True: # Infinite loop
            # Create a small batch of bookings
            batch_size = random.randint(1, 5)
            if client:
                event_data_batch = client.create_batch()
            
            for _ in range(batch_size):
                hotel = random.choice(hotels_list)
                booking = generate_booking(hotel)
                payload = json.dumps(booking)
                
                if client:
                    try:
                        event_data_batch.add(EventData(payload))
                    except ValueError:
                        # Batch full
                        client.send_batch(event_data_batch)
                        event_data_batch = client.create_batch()
                        event_data_batch.add(EventData(payload))
                else:
                    print(f"[TEST] {payload}")
            
            if client and len(event_data_batch) > 0:
                client.send_batch(event_data_batch)
                print(f"📨 {batch_size} Rezervasyon gönderildi...")

            time.sleep(DELAY_SECONDS)

    except KeyboardInterrupt:
        print("\nSimülasyon durduruldu.")
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    run_producer()
