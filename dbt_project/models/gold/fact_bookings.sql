/*
    FACT_BOOKINGS — Gold Layer
    ==========================
    Ana fact tablosu: her satır bir booking transaction.
    
    Filtreler:
    - booking_date > 1950 (1900-01-01 sentinel values hariç)
    - hotel_id IS NOT NULL (eski stream kayıtları hariç)
    
    KPI:
    - total_amount_eur: Tüm para birimlerini EUR'a çevirir
    - avg_nightly_rate: Gecelik ortalama ücret
*/

WITH bookings AS (
    SELECT *
    FROM {{ source('silver', 'silver_bookings') }}
    WHERE booking_date > DATE '1950-01-01'
      AND hotel_id IS NOT NULL
      AND booking_id IS NOT NULL
),

city_dim AS (
    SELECT *
    FROM {{ source('silver', 'silver_city_dim') }}
)

SELECT
    -- === Keys ===
    b.booking_id,
    b.hotel_id,
    b.customer_id,
    
    -- === Date Keys (for dim_date join) ===
    b.booking_date,
    YEAR(b.booking_date)                    AS booking_year,
    MONTH(b.booking_date)                   AS booking_month,
    
    -- === Location ===
    b.city_clean                            AS city,
    b.country,
    
    -- === Hotel Info ===
    b.hotel_name,
    b.hotel_type,
    b.star_rating,
    
    -- === Booking Details ===
    b.room_type,
    b.nights,
    b.adults,
    b.children,
    b.booking_status,
    b.source_system,
    b.booking_channel,
    
    -- === Financial ===
    b.currency,
    b.total_amount,
    b.room_price,
    COALESCE(b.exchange_rate_to_eur, 1.0)   AS exchange_rate_to_eur,
    
    -- === KPI: Revenue in EUR ===
    ROUND(
        b.total_amount * COALESCE(b.exchange_rate_to_eur, 1.0), 2
    )                                       AS total_amount_eur,
    
    -- === KPI: Avg Nightly Rate ===
    CASE
        WHEN b.nights > 0 THEN ROUND(b.total_amount / b.nights, 2)
        ELSE b.total_amount
    END                                     AS avg_nightly_rate,
    
    -- === KPI: Avg Nightly Rate in EUR ===
    CASE
        WHEN b.nights > 0 THEN ROUND(
            (b.total_amount * COALESCE(b.exchange_rate_to_eur, 1.0)) / b.nights, 2
        )
        ELSE ROUND(b.total_amount * COALESCE(b.exchange_rate_to_eur, 1.0), 2)
    END                                     AS avg_nightly_rate_eur,
    
    -- === Weather (from enrichment) ===
    b.temperature_c,
    b.weather_code

FROM bookings b
