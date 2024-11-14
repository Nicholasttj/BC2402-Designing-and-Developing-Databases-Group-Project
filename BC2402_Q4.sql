/* Q4 */

/*
Step 1: Extract the month and define each route.
We use the `STR_TO_DATE` function to convert the Date column and extract the month. 
The Origin and Dest columns are concatenated to create a unique route identifier.
*/

WITH MonthlyRouteDelays AS (
    SELECT 
        MONTH(STR_TO_DATE(Date, '%d-%m-%Y')) AS Month, -- Extracting the month from the Date column
        CONCAT(Origin, '-', Dest) AS Route, -- Combining Origin and Dest to define each route
        COUNT(CASE WHEN ArrDelay > 0 THEN 1 END) AS DelayCount -- Count delays where ArrDelay > 0
    FROM 
        flight_delay
    GROUP BY 
        Month, Route
)

/*
Step 2: Next, I want to identify the route with the most number of delays for each month
*/
SELECT 
    Month, Route, DelayCount
FROM 
    MonthlyRouteDelays
WHERE 
    (Month, DelayCount) IN ( -- I used a subquery to find out what is the maximum delay count for each month
        SELECT 
            Month, MAX(DelayCount) --  I want to get only the maximum delay count per month
        FROM 
            MonthlyRouteDelays
        GROUP BY 
            Month
    )
ORDER BY 
    Month ASC; -- Order results by month

/*
Explanation:
1. The `MonthlyRouteDelays` CTE calculates delays for each route per month by counting instances where ArrDelay > 0.
2. I used a subquery to identify the route with the most number of delays for each month by selecting the maximum delay count.
3. The final result shows the route with the most number of delays for each month, ordered by month.
*/