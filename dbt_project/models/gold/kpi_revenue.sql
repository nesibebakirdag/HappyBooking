/*
    KPI_REVENUE — Gold Layer
    ========================
    Şehir bazlı gelir KPI tablosu.
    Power BI dashboard'unda revenue analizi için kullanılır.
    
    Metrikler:
    - total_revenue_eur: Toplam gelir (EUR)
    - avg_booking_value_eur: Ortalama booking değeri
    - total_bookings: Toplam rezervasyon sayısı
    - avg_nightly_rate_eur: Ortalama gecelik ücret
    - avg_nights: Ortalama konaklama süresi
    - cancellation_rate: İptal oranı (%)
*/

WITH fact AS (
    SELECT *
    FROM {{ ref('fact_bookings') }}
),

city AS (
    SELECT *
    FROM {{ ref('dim_city') }}
)

SELECT
    -- === Dimensions ===
    f.city,
    f.country,
    f.booking_year,
    f.booking_month,
    f.hotel_type,
    
    -- === Revenue KPIs ===
    COUNT(*)                                            AS total_bookings,
    ROUND(SUM(f.total_amount_eur), 2)                   AS total_revenue_eur,
    ROUND(AVG(f.total_amount_eur), 2)                   AS avg_booking_value_eur,
    ROUND(AVG(f.avg_nightly_rate_eur), 2)               AS avg_nightly_rate_eur,
    
    -- === Occupancy KPIs ===
    ROUND(AVG(f.nights), 1)                             AS avg_nights,
    SUM(f.adults + COALESCE(f.children, 0))             AS total_guests,
    
    -- === Cancellation KPI ===
    ROUND(
        SUM(CASE WHEN f.booking_status = 'Canceled' THEN 1 ELSE 0 END) * 100.0 
        / NULLIF(COUNT(*), 0), 1
    )                                                   AS cancellation_rate,
    
    -- === Channel Mix ===
    COUNT(DISTINCT f.booking_channel)                    AS channel_count,
    
    -- === Weather Context ===
    c.temperature_c                                     AS city_temperature_c,
    c.weather_code                                      AS city_weather_code

FROM fact f
LEFT JOIN city c ON f.city = c.city
GROUP BY
    f.city, f.country, f.booking_year, f.booking_month,
    f.hotel_type, c.temperature_c, c.weather_code
