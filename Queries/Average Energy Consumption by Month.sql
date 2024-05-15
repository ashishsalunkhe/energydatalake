-- Understanding Seasonal Variations in Electricity Demand:
-- How does Ercot's electricity demand vary throughout the year?
SELECT 
    EXTRACT(MONTH FROM INTERVAL_START) AS month,
    AVG(load) AS average_load
FROM 
    ercot_merged.ercot_fm_load_merged
GROUP BY 
    month
ORDER BY 
    month;
