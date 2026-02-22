/*
    DIM_HOTEL — Gold Layer
    ======================
    Otel boyut tablosu: distinct hotel bilgileri silver_bookings'den.
*/

WITH hotels AS (
    SELECT DISTINCT
        hotel_id,
        FIRST_VALUE(hotel_name)   OVER (PARTITION BY hotel_id ORDER BY booking_date DESC) AS hotel_name,
        FIRST_VALUE(city_clean)   OVER (PARTITION BY hotel_id ORDER BY booking_date DESC) AS city,
        FIRST_VALUE(country)      OVER (PARTITION BY hotel_id ORDER BY booking_date DESC) AS country,
        FIRST_VALUE(hotel_type)   OVER (PARTITION BY hotel_id ORDER BY booking_date DESC) AS hotel_type,
        FIRST_VALUE(star_rating)  OVER (PARTITION BY hotel_id ORDER BY booking_date DESC) AS star_rating,
        FIRST_VALUE(latitude)     OVER (PARTITION BY hotel_id ORDER BY booking_date DESC) AS latitude,
        FIRST_VALUE(longitude)    OVER (PARTITION BY hotel_id ORDER BY booking_date DESC) AS longitude
    FROM {{ source('silver', 'silver_bookings') }}
    WHERE hotel_id IS NOT NULL
)

SELECT DISTINCT
    -- === Keys ===
    hotel_id,
    
    -- === Attributes ===
    hotel_name,
    city,
    country,
    hotel_type,
    star_rating,
    latitude,
    longitude,

    -- === Derived ===
    CASE
        WHEN star_rating >= 4 THEN 'Premium'
        WHEN star_rating >= 3 THEN 'Standard'
        ELSE 'Budget'
    END AS hotel_tier

FROM hotels
