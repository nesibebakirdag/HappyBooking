/*
    DIM_CITY — Gold Layer
    =====================
    Şehir boyut tablosu: weather + exchange rates.
    silver_city_dim tablosundan gelir.
*/

WITH city_raw AS (
    SELECT *
    FROM {{ source('silver', 'silver_city_dim') }}
    WHERE city IS NOT NULL
      AND city != 'Unknown'
)

SELECT
    -- === Surrogate Key ===
    ROW_NUMBER() OVER (ORDER BY city)       AS city_key,
    
    -- === Location ===
    city,
    country,
    latitude,
    longitude,
    
    -- === Weather ===
    temperature_c,
    humidity,
    weather_code,
    
    -- === Exchange Rates (all pivoted) ===
    COALESCE(rate_usd, 0)                   AS rate_usd,
    COALESCE(rate_gbp, 0)                   AS rate_gbp,
    COALESCE(rate_jpy, 0)                   AS rate_jpy,
    COALESCE(rate_try, 0)                   AS rate_try,
    COALESCE(rate_aed, 0)                   AS rate_aed,
    COALESCE(rate_cny, 0)                   AS rate_cny

FROM city_raw
