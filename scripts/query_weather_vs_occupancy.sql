-- 2. Analiza korelacji między pogodą (deszcz / temperatura) a dostępnością rowerów
-- Sprawdza średnie zapełnienie stacji w zależności od warunków atmosferycznych
SELECT 
    CASE 
        WHEN rain > 0 THEN 'Deszczowo (Opad > 0mm)'
        ELSE 'Sucho'
    END as weather_condition,
    CASE 
        WHEN temp < 10 THEN 'Zimno (< 10°C)'
        WHEN temp BETWEEN 10 AND 20 THEN 'Umiarkowanie (10-20°C)'
        ELSE 'Ciepło (> 20°C)'
    END as temperature_range,
    ROUND(AVG(occupancy_rate)::numeric, 2) as avg_occupancy_pct,
    ROUND(AVG(bikes_available)::numeric, 2) as avg_bikes_available,
    COUNT(*) as number_of_samples
FROM station_status
GROUP BY 
    CASE WHEN rain > 0 THEN 'Deszczowo (Opad > 0mm)' ELSE 'Sucho' END,
    CASE 
        WHEN temp < 10 THEN 'Zimno (< 10°C)'
        WHEN temp BETWEEN 10 AND 20 THEN 'Umiarkowanie (10-20°C)'
        ELSE 'Ciepło (> 20°C)'
    END
ORDER BY avg_occupancy_pct DESC;
