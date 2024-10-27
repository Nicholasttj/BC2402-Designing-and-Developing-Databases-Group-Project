SELECT
    MONTH(STR_TO_DATE(Date, '%d-%m-%Y')) AS Month,
    CONCAT(Origin, '-', Dest) AS Route,
    COUNT(*) AS DelayCount
FROM
    Flight_delay
WHERE
    CarrierDelay > 0
    OR WeatherDelay > 0
    OR NASDelay > 0
    OR SecurityDelay > 0
    OR LateAircraftDelay > 0
GROUP BY
    Month, Route
ORDER BY
    Month, DelayCount DESC;