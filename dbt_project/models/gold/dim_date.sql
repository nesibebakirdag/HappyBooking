/*
    DIM_DATE — Gold Layer
    =====================
    Tarih boyut tablosu: Power BI Time Intelligence için.
    2020-01-01 → 2026-12-31 arası tüm günleri kapsar.
*/

WITH date_spine AS (
    SELECT
        EXPLODE(
            SEQUENCE(
                DATE '2020-01-01',
                DATE '2026-12-31',
                INTERVAL 1 DAY
            )
        ) AS date_day
)

SELECT
    -- === Key ===
    date_day                                        AS date_key,
    
    -- === Calendar ===
    YEAR(date_day)                                  AS year,
    MONTH(date_day)                                 AS month,
    DAY(date_day)                                   AS day,
    QUARTER(date_day)                               AS quarter,
    
    -- === Labels ===
    DATE_FORMAT(date_day, 'MMMM')                   AS month_name,
    DATE_FORMAT(date_day, 'MMM')                     AS month_short,
    DATE_FORMAT(date_day, 'EEEE')                    AS day_name,
    DATE_FORMAT(date_day, 'EEE')                     AS day_short,
    
    -- === ISO ===
    WEEKOFYEAR(date_day)                            AS week_of_year,
    DAYOFWEEK(date_day)                             AS day_of_week,
    DAYOFYEAR(date_day)                             AS day_of_year,
    
    -- === Flags ===
    CASE
        WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE
        ELSE FALSE
    END                                             AS is_weekend,
    
    -- === Period Labels (Power BI Slicers) ===
    CONCAT(YEAR(date_day), '-Q', QUARTER(date_day)) AS year_quarter,
    DATE_FORMAT(date_day, 'yyyy-MM')                AS year_month

FROM date_spine
