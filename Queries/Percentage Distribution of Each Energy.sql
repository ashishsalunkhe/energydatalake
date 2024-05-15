-- How does each energy source contribute to the overall energy mix, expressed as a percentage?
-- This query calculates the percentage contribution of each energy source to the total energy mix, rounded to two decimal places.


SELECT 
    ROUND(SUM(coal_and_lignite) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS coal_and_lignite_percent,
    ROUND(SUM(hydro) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS hydro_percent,
    ROUND(SUM(nuclear) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS nuclear_percent,
    ROUND(SUM(power_storage) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS power_storage_percent,
    ROUND(SUM(solar) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS solar_percent,
    ROUND(SUM(wind) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS wind_percent,
    ROUND(SUM(natural_gas) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS natural_gas_percent,
    ROUND(SUM(other) / SUM(coal_and_lignite + hydro + nuclear + power_storage + solar + wind + natural_gas + other) * 100, 2) AS other_percent
FROM 
    `ercot_merged.ercot_fm_load_merged`;

