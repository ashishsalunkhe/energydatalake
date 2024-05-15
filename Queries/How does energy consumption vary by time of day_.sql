SELECT 
    EXTRACT(HOUR FROM INTERVAL_START) AS hour_of_day,
    AVG(load) AS average_load
FROM 
    ercot_merged.ercot_fm_load_merged
GROUP BY 
    hour_of_day
ORDER BY 
    hour_of_day;
