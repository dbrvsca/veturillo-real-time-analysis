-- 1. Najczęściej puste stacje (Top 10 stacji o najniższym średnim zapełnieniu)
SELECT 
    station_id,
    name,
    ROUND(AVG(occupancy_rate)::numeric, 2) as avg_occupancy_pct,
    MIN(bikes_available) as min_bikes,
    MAX(bikes_available) as max_bikes,
    COUNT(*) as number_of_records
FROM station_status
GROUP BY station_id, name
ORDER BY avg_occupancy_pct ASC
LIMIT 10;
