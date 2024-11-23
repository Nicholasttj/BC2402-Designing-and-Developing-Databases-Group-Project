SELECT * from airlines_reviews;
select * from customer_booking;
select * from customer_support; 
select * from flight_delay; 
select * from sia_stock; 

/*--------------------------------------------------------------------------------------------------------------------------------------------*/

/* Q1 */ 

SELECT DISTINCT(category) 
FROM customer_support;

SELECT COUNT(DISTINCT(category)) AS number_of_categories_in_customer_support 
FROM customer_support
WHERE category IS NOT NULL
  AND TRIM(category) != '';
  
SELECT DISTINCT(category)
FROM customer_support
WHERE category IS NOT NULL
  AND TRIM(category) != '' 
  AND category = UPPER(category)  
  AND category NOT LIKE '% %';  

SELECT COUNT(DISTINCT(category)) AS number_of_categories_in_customer_support 
FROM customer_support
WHERE category IS NOT NULL
  AND TRIM(category) != '' 
  AND category = UPPER(category)  
  AND category NOT LIKE '% %';  

/* 
Firstly, looked at the different categories and saw that several of them were incorrectly classified. 
Secondly, there are 36 different categories, according to the count. 
According to my analysis, only the uppercase letters and one word represent the different categories and the other data is incorrectly entered. 
Thirdly, in order to verify that it is accurate, I only return rows with a category that is all capital, disregard empty cells, and rows that contain sentences. 
Lastly, I counted the distinct number of categories, which give me a result of 8 categories, after making sure it is of the distinct categories. 
*/ 

/*--------------------------------------------------------------------------------------------------------------------------------------------*/

/*Q2*/

/*The question is aim to count the number of records in the customer_support table that contain specific language tags (e.g., offensive, colloquial, slang, abusive), and then group them by category.*/

SELECT DISTINCT response
FROM customer_support;
SELECT DISTINCT category
FROM customer_support;
/*from the above query, The category column before the data cleaning have 36 different categories.
 In the 36 categories, omly 8 categories with upper case are the correct categories. The other categories are not correct but is not
 related to the question. Hence, data clean is not needed in this stage.
The next step, we need to find out the records that contained colloquial variation and offensive language.
To solve this, we can use the language generation tags from response.
By count the specific language generation tags from response column, we can figure out the number of records that contained colloquial variation and offensive language.
After this, we just need to group the counts by category*/
/*According to the process, the solution is shown below*/
SELECT category,
       COUNT(*) AS records_with_colloquial_or_offensive_language
FROM customer_support
WHERE response REGEXP 'offensive|colloquial|slang|abusive'
GROUP BY category;

/*--------------------------------------------------------------------------------------------------------------------------------------------*/

/* Q3 */

SELECT DISTINCT Cancelled
FROM flight_delay;

/*
From the above query, there is only one distinct value in the Cancelled column, which is 0. Looking at the data dictionary,
this corresponds to flights not cancelled. This means that there is no cancellation for any of the flights listed in this dataset.
*/

/*
Exploring the data, I noticed that the arrival delay column is equal to the sum of carrier delay, weather delay, NAS delay, security
delay, and late aircraft delay. That is, ArrDelay = CarrierDelay + WeatherDelay + NASDelay + SecurityDelay + LateAircraftDelay. 
Hence, in counting the instances of delays, the ArrDelay column will be considered and the latter 5 columns will be ignored, to prevent
double-counting.
*/

/*
I also saw a pattern between the arrival delay column with other columns. Particularly, the following equation is derived:
ArrDelay = DepDelay + ActualElapsedTime - CRSElapsed Time. That is, some flights took a shorter or longer flight time than the 
estimated/allocated time, hence causing offsetting or worsening the initial departure delay respectively. Similarly, only the
ArrDelay column will thus be considered to prevent double-counting.
*/

/*
The instances of delay will be based on the ArrDelay column, so long as the value in that column is more than 0.
This also makes sense as airlines are usually more concerned about their aircraft arrives at the destination airport later than
it is expected to. Departing late from the origin airport is not as big a concern as the aircraft may be able to catch up on lost
time in the air.
*/

SELECT DISTINCT Airline
FROM flight_delay;

/*
I checked for the Airline column to ensure that there is no data issues. There are 12 distinct airlines.
For other columns, I will not do any data cleaning since I will not be using those columns for this question.
*/

SELECT *
FROM flight_delay
WHERE ArrDelay=0;

SELECT Airline, COUNT(ArrDelay) as no_of_delays, (COUNT(Cancelled)-COUNT(Cancelled = 0)) AS no_of_cancellations
FROM flight_delay
GROUP BY Airline
ORDER BY Airline ASC;

/*
I further set a threshold of ArrDelay of more than 60 minutes to be considered a severe delay.
I calculate the number of severe delays as a proportion of total delays for each airline,
and express them as a percentage.
*/

SELECT severe_delay.Airline, no_of_severe_delays, no_of_delays, CONCAT(ROUND((no_of_severe_delays/no_of_delays*100),0), "%") AS percentage_of_severe_delays
FROM
	(
	SELECT Airline, COUNT(ArrDelay) as no_of_severe_delays
	FROM flight_delay
	WHERE ArrDelay > 60
	GROUP BY Airline
	ORDER BY Airline ASC
	)
	AS severe_delay

	INNER JOIN

	(
	SELECT Airline, COUNT(ArrDelay) as no_of_delays
	FROM flight_delay
	GROUP BY Airline
	ORDER BY Airline ASC
	)
	AS total_delay

ON severe_delay.Airline = total_delay.Airline
ORDER BY percentage_of_severe_delays DESC;

/*--------------------------------------------------------------------------------------------------------------------------------------------*/

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

/*--------------------------------------------------------------------------------------------------------------------------------------------*/

/* 5 */

# - [sia_stock] For the year 2023, display the quarter-on-quarter changes in high and low prices and the quarterly average price.

# Calculate the highest, lowest, and average prices for each quarter of 2023
WITH QuarterlyData AS (
    SELECT 
        QUARTER(STR_TO_DATE(StockDate, '%m/%d/%Y')) AS quarter,
        MAX(High) AS max_high,
        MIN(Low) AS min_low,
        ROUND(AVG(Price), 2) AS avg_price
    FROM sia_stock
    WHERE YEAR(STR_TO_DATE(StockDate, '%m/%d/%Y')) = 2023
    GROUP BY QUARTER(STR_TO_DATE(StockDate, '%m/%d/%Y'))
)

# Join each quarter's data with the previous quarter to calculate QoQ changes
SELECT 
    curr.quarter AS current_quarter,
    curr.max_high AS current_max_high,
    curr.min_low AS current_min_low,
    curr.avg_price AS current_avg_price,
    CONCAT(ROUND(((curr.max_high - prev.max_high) / prev.max_high) * 100, 2), '%') AS qoq_high_change,
    CONCAT(ROUND(((curr.min_low - prev.min_low) / prev.min_low) * 100, 2), '%') AS qoq_low_change,
    CONCAT(ROUND(((curr.avg_price - prev.avg_price) / prev.avg_price) * 100, 2), '%') AS qoq_avg_price_change  -- QoQ change for average price with %
FROM 
    QuarterlyData AS curr
LEFT JOIN 
    QuarterlyData AS prev 
ON 
    curr.quarter = prev.quarter + 1;
    
# The result for Quarter 1 on QOQ changes will display as NULL because we are comparing the quarters only in the year of 2023. 
# Hence, Quarters 2,3,4 will be able to compare its results to the previous quarter, but quarter 1 is the first quarter of the year and do not have previous quarter to compare against.
# Also, the QOQ changes shows the calculated percentage change compared to the previous quarter.

# Findings 1: Q2 saw a general increase across the board, with significant gains in both the highest and average prices. 
# This could suggest increased market activity or demand during this period.

# Findings 2: For Q3, the average price continued to rise despite a dip in the highest price, suggesting more stability in pricing with fewer extreme highs. 
# The rise in the lowest price indicates that the overall price range shifted upwards. 

# Findings 3: Q4 saw a downward trend in prices across all metrics. The declines in both high and low prices suggest reduced market activity or 
# demand in the final quarter of the year.

# This QOQ analysis suggests fluctuations in prices, with a peak around Q2 and a gradual decline towards the end of the year. 
# This pattern could be seasonal or reflect broader market trends impacting prices throughout the year.

/*--------------------------------------------------------------------------------------------------------------------------------------------*/

/*Q6*/

select*
from customer_booking;
SELECT
    sales_channel,
    route,        

    -- Compute the average length of stay per flight hour.
    -- If the average flight hour is zero, return NULL to avoid division by zero errors.
    CASE 
        WHEN AVG(flight_duration) = 0 THEN NULL 
        ELSE AVG(length_of_stay) / AVG(flight_duration) 
    END AS avg_length_of_stay_per_flight_hour,

    -- Compute the average wants for extra baggage per flight hour.
    -- Handle division by zero similarly.
    CASE 
        WHEN AVG(flight_duration) = 0 THEN NULL 
        ELSE AVG(wants_extra_baggage) / AVG(flight_duration) 
    END AS avg_wants_extra_baggage_per_flight_hour,

    -- Compute the average wants for a preferred seat per flight hour.
    -- Handle division by zero similarly.
    CASE 
        WHEN AVG(flight_duration) = 0 THEN NULL 
        ELSE AVG(wants_preferred_seat) / AVG(flight_duration) 
    END AS avg_wants_preferred_seat_per_flight_hour,

    -- Compute the average wants for in-flight meals per flight hour.
    -- Handle division by zero similarly.
    CASE 
        WHEN AVG(flight_duration) = 0 THEN NULL 
        ELSE AVG(wants_in_flight_meals) / AVG(flight_duration) 
    END AS avg_wants_in_flight_meals_per_flight_hour

FROM
    customer_booking -- The table containing customer booking data.

GROUP BY
    sales_channel,   -- Group by sales channel to aggregate metrics for each channel.
    route;           -- Group by route to aggregate metrics for each flight route.
    
/*--------------------------------------------------------------------------------------------------------------------------------------------*/

/* Q7 */
# - [airlines_reviews] Airline seasonality.
# For each Airline and Class, display the averages of SeatComfort, FoodnBeverages, InflightEntertainment, ValueForMoney, and OverallRating 
# for the seasonal and non-seasonal periods, respectively.
# Note: June to September is seasonal, while the remaining period is non-seasonal.

# Seasonal Period (June to September)
# Calculating the average rating for each category in the different class in each airline
WITH Seasonal AS (
    SELECT 
        Airline,
        Class,
        'Seasonal' AS Season,
        ROUND(AVG(SeatComfort), 2) AS avg_seat_comfort,
        ROUND(AVG(FoodnBeverages), 2) AS avg_food_beverages,
        ROUND(AVG(InflightEntertainment), 2) AS avg_inflight_entertainment,
        ROUND(AVG(ValueForMoney), 2) AS avg_value_for_money,
        ROUND(AVG(OverallRating), 2) AS avg_overall_rating
    FROM airlines_reviews
    WHERE MonthFlown IN ('Jun-23', 'Jul-23', 'Aug-23', 'Sep-23', 'Jun-24', 'Jul-24', 'Aug-24', 'Sep-24')
    GROUP BY Airline, Class
),

# Non-Seasonal Period (January to May & October to December)
# Calculating the average rating for each category in the different class in each airline
NonSeasonal AS (
    SELECT 
        Airline,
        Class,
        'Non-Seasonal' AS Season,
        ROUND(AVG(SeatComfort), 2) AS avg_seat_comfort,
        ROUND(AVG(FoodnBeverages), 2) AS avg_food_beverages,
        ROUND(AVG(InflightEntertainment), 2) AS avg_inflight_entertainment,
        ROUND(AVG(ValueForMoney), 2) AS avg_value_for_money,
        ROUND(AVG(OverallRating), 2) AS avg_overall_rating
    FROM airlines_reviews
    WHERE MonthFlown NOT IN ('Jun-23', 'Jul-23', 'Aug-23', 'Sep-23', 'Jun-24', 'Jul-24', 'Aug-24', 'Sep-24')
    GROUP BY Airline, Class
)

# Combine the seasonal and non-seasonal results
SELECT * FROM Seasonal
UNION ALL
SELECT * FROM NonSeasonal;

/*--------------------------------------------------------------------------------------------------------------------------------------------*/

/* 8 */
WITH CategorizedComplaints AS (
    -- Step 1: Categorize complaints based on keywords in the Reviews
    -- For each review, we assign a complaint type based on the presence of specific keywords
    SELECT 
        Airline,  -- Airline for which the review is provided
        TypeofTraveller,  -- Type of traveller (e.g., Business, Leisure)
        CASE
            WHEN Reviews LIKE '%delay%' THEN 'Delay'  -- Complaints related to flight delays
            WHEN Reviews LIKE '%lost baggage%' OR Reviews LIKE '%luggage%' THEN 'Lost Baggage'  -- Complaints about lost luggage
            WHEN Reviews LIKE '%rude%' OR Reviews LIKE '%unfriendly%' THEN 'Rude Staff'  -- Complaints about rude or unfriendly staff
            WHEN Reviews LIKE '%legroom%' OR Reviews LIKE '%cramped%' THEN 'Lack of Comfort'  -- Complaints about lack of legroom or cramped spaces
            WHEN Reviews LIKE '%food%' OR Reviews LIKE '%meal%' THEN 'Poor Food Quality'  -- Complaints about food quality
            ELSE 'Other'  -- Any complaints that do not fit into the specified categories
        END AS ComplaintType  -- Categorize each review based on the keywords
    FROM 
        airlines_reviews  -- From the airlines_reviews table
),
ComplaintCounts AS (
    -- Step 2: Count occurrences of each complaint type per airline and type of traveller
    -- This step counts the frequency of each complaint type for each airline and traveller type
    SELECT 
        Airline,  -- Airline
        TypeofTraveller,  -- Type of Traveller
        ComplaintType,  -- Complaint type identified in Step 1
        COUNT(*) AS Complaint_Frequency  -- Count how many times each complaint type occurs
    FROM 
        CategorizedComplaints  -- From the categorized complaints table (Step 1)
    GROUP BY 
        Airline, TypeofTraveller, ComplaintType  -- Group by Airline, Type of Traveller, and Complaint Type
),
RankedComplaints AS (
    -- Step 3: Rank complaints based on frequency per airline and type of traveller
    -- Use ROW_NUMBER to rank complaints by their frequency (most frequent complaints rank highest)
    SELECT 
        Airline,  -- Airline
        TypeofTraveller,  -- Type of Traveller
        ComplaintType,  -- Complaint Type
        Complaint_Frequency,  -- Frequency of the Complaint Type
        ROW_NUMBER() OVER (PARTITION BY Airline, TypeofTraveller ORDER BY Complaint_Frequency DESC) AS Ranking  -- Rank the complaints within each airline and traveller type
    FROM 
        ComplaintCounts  -- From the complaint counts table (Step 2)
)

-- Step 4: Select top 5 complaints for each airline and type of traveller
SELECT 
    Airline,  -- Airline
    TypeofTraveller,  -- Type of Traveller
    
    -- Complaint 1: The most frequent complaint for each airline and traveller type
    MAX(CASE WHEN Ranking = 1 THEN ComplaintType END) AS Top_Complaint_1,
    MAX(CASE WHEN Ranking = 1 THEN Complaint_Frequency END) AS Complaint_Count_1,
    
    -- Complaint 2: The second most frequent complaint for each airline and traveller type
    MAX(CASE WHEN Ranking = 2 THEN ComplaintType END) AS Top_Complaint_2,
    MAX(CASE WHEN Ranking = 2 THEN Complaint_Frequency END) AS Complaint_Count_2,

    -- Complaint 3: The third most frequent complaint for each airline and traveller type
    MAX(CASE WHEN Ranking = 3 THEN ComplaintType END) AS Top_Complaint_3,
    MAX(CASE WHEN Ranking = 3 THEN Complaint_Frequency END) AS Complaint_Count_3,

    -- Complaint 4: The fourth most frequent complaint for each airline and traveller type
    MAX(CASE WHEN Ranking = 4 THEN ComplaintType END) AS Top_Complaint_4,
    MAX(CASE WHEN Ranking = 4 THEN Complaint_Frequency END) AS Complaint_Count_4,

    -- Complaint 5: The fifth most frequent complaint for each airline and traveller type
    MAX(CASE WHEN Ranking = 5 THEN ComplaintType END) AS Top_Complaint_5,
    MAX(CASE WHEN Ranking = 5 THEN Complaint_Frequency END) AS Complaint_Count_5

FROM 
    RankedComplaints  -- From the ranked complaints table (Step 3)
WHERE 
    Ranking <= 5  -- We only want the top 5 complaints for each airline and traveller type
GROUP BY 
    Airline, TypeofTraveller  -- Group results by Airline and Type of Traveller
ORDER BY 
    Airline, TypeofTraveller;  -- Order the results by Airline and Type of Traveller for easy readability
    
/*--------------------------------------------------------------------------------------------------------------------------------------------*/

/* Q9 */

/* Recommended with Individual Ratings */
SELECT 
    CASE 
        WHEN STR_TO_DATE(ReviewDate, '%d/%m/%Y') < '2020-03-11' THEN 'Pre-COVID'
        WHEN STR_TO_DATE(ReviewDate, '%d/%m/%Y') BETWEEN '2020-03-11' AND '2022-04-04' THEN 'Peri-COVID'
        ELSE 'Post-COVID'
    END AS Period,
    ROUND(AVG(OverallRating), 2) AS AvgRating,  
    SUM(CASE WHEN LOWER(Recommended) = 'yes' THEN 1 ELSE 0 END) AS NumRecommended,
    COUNT(*) AS TotalReviews,
    CONCAT(ROUND((SUM(CASE WHEN StaffService >= 3 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2), '%') AS ServiceRelated,
    CONCAT(ROUND((SUM(CASE WHEN ValueForMoney >= 3 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2), '%') AS PriceRelated,
    CONCAT(ROUND((SUM(CASE WHEN SeatComfort >= 3 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2), '%') AS SeatRelated,
    CONCAT(ROUND((SUM(CASE WHEN FoodnBeverages >= 3 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2), '%') AS FoodRelated,
    CONCAT(ROUND((SUM(CASE WHEN LOWER(Recommended) = 'yes' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2), '%') AS PercentageRecommended
FROM 
    airlines_reviews
WHERE 
    Airline = 'Singapore Airlines'
GROUP BY 
    Period;

/* 
Categorize each review by date into Pre-COVID (2020-03-11), Peri-COVID (between '2020-03-11' and '2022-04-04'), or Post-COVID periods (as long as it is not within the Pre-COVID and Peri COVID). 
Round the average rating for each period to two decimal places.
Count the number of reviews where the recommendation is 'yes', and the total number of reviews for each period.
Then calculate the percentage of reviews for ServiceRelated, PriceRelated, SeatRelated, FoodRelated rating that is above and equal to 3 which indicate satisfaction.
Retrieve the results from table containing airline reviews.
Group the results by period (Pre-COVID, Peri-COVID, Post-COVID).
*/ 

/* Not Recommended with Individual Ratings */
SELECT 
    CASE 
        WHEN STR_TO_DATE(ReviewDate, '%d/%m/%Y') < '2020-03-11' THEN 'Pre-COVID'
        WHEN STR_TO_DATE(ReviewDate, '%d/%m/%Y') BETWEEN '2020-03-11' AND '2022-04-04' THEN 'Peri-COVID'
        ELSE 'Post-COVID'
    END AS Period,
    ROUND(AVG(OverallRating), 2) AS AvgRating,  
    SUM(CASE WHEN LOWER(Recommended) = 'no' THEN 1 ELSE 0 END) AS NumNotRecommended,
    COUNT(*) AS TotalReviews,
    CONCAT(ROUND((SUM(CASE WHEN StaffService < 3 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2), '%') AS ServiceRelated,
    CONCAT(ROUND((SUM(CASE WHEN ValueForMoney < 3 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2), '%') AS PriceRelated,
    CONCAT(ROUND((SUM(CASE WHEN SeatComfort < 3 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2), '%') AS SeatRelated,
    CONCAT(ROUND((SUM(CASE WHEN FoodnBeverages < 3 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2), '%') AS FoodRelated,
    CONCAT(ROUND((SUM(CASE WHEN LOWER(Recommended) = 'no' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2), '%') AS PercentageNotRecommended
FROM 
    airlines_reviews
WHERE 
    Airline = 'Singapore Airlines'
GROUP BY 
    Period;

/* 
Categorize each review by date into Pre-COVID (2020-03-11), Peri-COVID (between '2020-03-11' and '2022-04-04'), or Post-COVID periods (as long as it is not within the Pre-COVID and Peri COVID). 
Round the average rating for each period to two decimal places.
Count the number of reviews where the recommendation is 'no', and the total number of reviews for each period.
Then calculate the percentage of reviews for ServiceRelated, PriceRelated, SeatRelated, FoodRelated rating that is below 3 which indicate dissatisfaction.
Retrieve the results from table containing airline reviews.
Group the results by period (Pre-COVID, Peri-COVID, Post-COVID).
*/ 

/* Common Words Across All Recommended Reviews */ 
DROP TABLE categorized_reviews;
DROP TABLE tokenized_words;
DROP PROCEDURE TokenizeWords;
 
CREATE TEMPORARY TABLE categorized_reviews AS
SELECT 
    CASE 
        WHEN STR_TO_DATE(ReviewDate, '%d/%m/%Y') < '2020-03-11' THEN 'Pre-COVID'
        WHEN STR_TO_DATE(ReviewDate, '%d/%m/%Y') BETWEEN '2020-03-11' AND '2022-04-04' THEN 'Peri-COVID'
        ELSE 'Post-COVID'
    END AS Period,
    LOWER(REGEXP_REPLACE(Reviews, '[^a-zA-Z0-9\s]+', ' ')) AS ReviewText
FROM 
    airlines_reviews
WHERE 
    Airline = 'Singapore Airlines' 
    AND Recommended = 'yes';

CREATE TEMPORARY TABLE tokenized_words (
    Period VARCHAR(50),
    Word VARCHAR(50)
);

DELIMITER //

CREATE PROCEDURE TokenizeWords()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE review_text VARCHAR(5000);
    DECLARE review_period VARCHAR(50);
    DECLARE cur CURSOR FOR SELECT Period, ReviewText FROM categorized_reviews;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO review_period, review_text;
        IF done THEN
            LEAVE read_loop;
        END IF;

        WHILE LOCATE(' ', review_text) > 0 DO
            SET @word = SUBSTRING_INDEX(review_text, ' ', 1);
            SET review_text = SUBSTRING(review_text, LOCATE(' ', review_text) + 1);
            IF LENGTH(@word) > 1 AND @word NOT IN ('i', 'and', 'was', 'to', 'a', 'in', 'of', 'on', 'with', 'for', 'is', 'were', 'my', 'it', 'not', 'we', 'that', 'but', 'this', 'they', 'very', 'as', 'singapore', 'airlines', 'from', 'had', 'flight', 'at', 'have', 'no', 'an', 'be', 't', 'so', 'the', 'me', 'you', 'our', 'are', 'by', 'only','good','when', 'there' , 'their' , 'which' , 'or', 'would') THEN
                INSERT INTO tokenized_words (Period, Word) VALUES (review_period, @word);
            END IF;
        END WHILE;

        IF LENGTH(review_text) > 1 AND review_text NOT IN ('i', 'and', 'was', 'to', 'a', 'in', 'of', 'on', 'with', 'for', 'is', 'were', 'my', 'it', 'not', 'we', 'that', 'but', 'this', 'they', 'very', 'as', 'singapore', 'airlines', 'from', 'had', 'flight', 'at', 'have', 'no', 'an', 'be', 't', 'so', 'the', 'me', 'you', 'our', 'are', 'by', 'only','good', 'when', 'there', 'their' , 'which' , 'or', 'would') THEN
            INSERT INTO tokenized_words (Period, Word) VALUES (review_period, review_text);
        END IF;
    END LOOP;

    CLOSE cur;
END //

DELIMITER ;

CALL TokenizeWords();

SELECT 
    Word,
    SUM(CASE WHEN Period = 'Pre-COVID' THEN 1 ELSE 0 END) AS Pre_COVID,
    SUM(CASE WHEN Period = 'Peri-COVID' THEN 1 ELSE 0 END) AS Peri_COVID,
    SUM(CASE WHEN Period = 'Post-COVID' THEN 1 ELSE 0 END) AS Post_COVID
FROM 
    tokenized_words
GROUP BY 
    Word
ORDER BY 
    (Pre_COVID + Peri_COVID + Post_COVID) DESC
LIMIT 10;

/* 
Drop any tables categorized_reviews and tokenized_words, and stored procedure TokenizeWords if already there.
Create temporary table, categorized_reviews, to sort reviews by date and clean text for next steps.
Change Reviews text to lowercase and remove special characters like non-alphabet numbers using REGEXP_REPLACE to make it simple.
Use CASE to sort reviews into 'Pre-COVID,' 'Peri-COVID,' or 'Post-COVID' based on ReviewDate.
Choose only the reviews where Airline is 'Singapore Airlines' and Recommended is 'yes'.

Create new temporary table, tokenized_words, to keep words from review text with the period.
Set delimiter to // so can make stored procedure without stopping at default ;.
Start writing procedure TokenizeWords to cut reviews into words and put them in tokenized_words.
Make cursor cur to go through each row in categorized_reviews, and handler to stop when no rows left.
Cut review_text into separate words such as for token, take first word from review_text into @word and remove it from review_text, 
if word length more than 1 and it’s not common word (like 'and', 'to', etc.), put @word and review_period into tokenized_words, 
and put last word if it fits rule, because last word not handled in loop.
*/ 

/* Common Words Across All Not Recommended Reviews */ 
DROP TABLE categorized_reviews;
DROP TABLE tokenized_words;
DROP PROCEDURE TokenizeWords;
 
CREATE TEMPORARY TABLE categorized_reviews AS
SELECT 
    CASE 
        WHEN STR_TO_DATE(ReviewDate, '%d/%m/%Y') < '2020-03-11' THEN 'Pre-COVID'
        WHEN STR_TO_DATE(ReviewDate, '%d/%m/%Y') BETWEEN '2020-03-11' AND '2022-04-04' THEN 'Peri-COVID'
        ELSE 'Post-COVID'
    END AS Period,
    LOWER(REGEXP_REPLACE(Reviews, '[^a-zA-Z0-9\s]+', ' ')) AS ReviewText
FROM 
    airlines_reviews
WHERE 
    Airline = 'Singapore Airlines' 
    AND Recommended = 'no';

CREATE TEMPORARY TABLE tokenized_words (
    Period VARCHAR(50),
    Word VARCHAR(50)
);

DELIMITER //

CREATE PROCEDURE TokenizeWords()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE review_text VARCHAR(5000);
    DECLARE review_period VARCHAR(50);
    DECLARE cur CURSOR FOR SELECT Period, ReviewText FROM categorized_reviews;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO review_period, review_text;
        IF done THEN
            LEAVE read_loop;
        END IF;

        WHILE LOCATE(' ', review_text) > 0 DO
            SET @word = SUBSTRING_INDEX(review_text, ' ', 1);
            SET review_text = SUBSTRING(review_text, LOCATE(' ', review_text) + 1);
            IF LENGTH(@word) > 1 AND @word NOT IN ('i', 'and', 'was', 'to', 'a', 'in', 'of', 'on', 'with', 'for', 'is', 'were', 'my', 'it', 'not', 'we', 'that', 'but', 'this', 'they', 'very', 'as', 'singapore', 'airlines', 'from', 'had', 'flight', 'at', 'have', 'no', 'an', 'be', 't', 'so', 'the', 'me', 'you', 'our', 'are', 'by', 'only','good','when', 'there' , 'their' , 'which' , 'or', 'would') THEN
                INSERT INTO tokenized_words (Period, Word) VALUES (review_period, @word);
            END IF;
        END WHILE;

        IF LENGTH(review_text) > 1 AND review_text NOT IN ('i', 'and', 'was', 'to', 'a', 'in', 'of', 'on', 'with', 'for', 'is', 'were', 'my', 'it', 'not', 'we', 'that', 'but', 'this', 'they', 'very', 'as', 'singapore', 'airlines', 'from', 'had', 'flight', 'at', 'have', 'no', 'an', 'be', 't', 'so', 'the', 'me', 'you', 'our', 'are', 'by', 'only','good', 'when', 'there', 'their' , 'which' , 'or', 'would') THEN
            INSERT INTO tokenized_words (Period, Word) VALUES (review_period, review_text);
        END IF;
    END LOOP;

    CLOSE cur;
END //

DELIMITER ;

CALL TokenizeWords();

SELECT 
    Word,
    SUM(CASE WHEN Period = 'Pre-COVID' THEN 1 ELSE 0 END) AS Pre_COVID,
    SUM(CASE WHEN Period = 'Peri-COVID' THEN 1 ELSE 0 END) AS Peri_COVID,
    SUM(CASE WHEN Period = 'Post-COVID' THEN 1 ELSE 0 END) AS Post_COVID
FROM 
    tokenized_words
GROUP BY 
    Word
ORDER BY 
    (Pre_COVID + Peri_COVID + Post_COVID) DESC
LIMIT 10;

/* 
Drop any tables categorized_reviews and tokenized_words, and stored procedure TokenizeWords if already there. 
Create temporary table, categorized_reviews, to sort reviews by date and clean text for next steps.
Change Reviews text to lowercase and remove special characters like non-alphabet numbers using REGEXP_REPLACE to make it simple.
Use CASE to sort reviews into 'Pre-COVID,' 'Peri-COVID,' or 'Post-COVID' based on ReviewDate.
Choose only the reviews where Airline is 'Singapore Airlines' and Recommended is 'no'. 
*/

/*
Create new temporary table, tokenized_words, to keep words from review text with the period.
Set delimiter to // so can make stored procedure without stopping at default ;.
Start writing procedure TokenizeWords to cut reviews into words and put them in tokenized_words.
Make cursor cur to go through each row in categorized_reviews, and handler to stop when no rows left.
Cut review_text into separate words such as for token, take first word from review_text into @word and remove it from review_text, 
if word length more than 1 and it’s not common word (like 'and', 'to', etc.), put @word and review_period into tokenized_words, 
and put last word if it fits rule, because last word not handled in loop.
*/ 

/*--------------------------------------------------------------------------------------------------------------------------------------------*/

/* Q10 */

/* Q10 */

/*
First, I identify the relevant issues from airlines_reviews.
I note that there are some reviews that are verified and some that are not.
I will do separate analyses, with one consisting of all reviews (both verified and non-verified),
and another one consisting only of verified reviews. I will analyse if there are any difference in the results.
I  create 2 temporary tables. The first table will store the results of the SELECT query.
For this query, I remove briefly the common special characters that show up in the Reviews column of airlines_reviews.
Using the REPLACE function, I overwrite those special characters with no characters.
I filter the data that is relevant to the question, which is Singapore Airlines.
I also further extracted only the reviews that came from those who did not recommend Singapore Airlines.
This is on the assumption that most, if not all, people will not recommend a particular product or service
if they have any complaints or unpleasant experiences. The reviews of people who usually recommend something
would be positive. Hence, to avoid diluting the data, only those reviews of not recommended are selected.
The next table is to create an empty table to store the outcome of my procedure.
*/

CREATE TEMPORARY TABLE Q10a
AS
SELECT LOWER(REPLACE(REPLACE(REPLACE(REPLACE(Reviews, '.', ''),',',''),'{',''),'}','')) AS ReviewText
FROM airlines_reviews
WHERE Airline = 'Singapore Airlines' 
    AND Recommended = 'no';

CREATE TEMPORARY TABLE Q10b (
    Word VARCHAR(50)
);

DELIMITER $$

/*
I create a procedure to break the content in the Reviews column of airlines_reviews into separate standalone words.
I use a while loop to do this.
I indicated a set of words to exclude when storing the outcomes in the empty table.
Those words are common words that do not pose great importance or purpose in explaining any insights.
By excluding those words, I prevent the results from being diluted, and I will be able to focus on the
main issues based from the complaints.
I then use CALL to run the procedure.
*/

CREATE PROCEDURE extractwordsall()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE review_text VARCHAR(5000);
    DECLARE cur CURSOR FOR SELECT ReviewText FROM Q10a;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO review_text;
        IF done THEN
            LEAVE read_loop;
        END IF;

        WHILE LOCATE(' ', review_text) > 0 DO
            SET @word = SUBSTRING_INDEX(review_text, ' ', 1);
            SET review_text = SUBSTRING(review_text, LOCATE(' ', review_text) + 1);
            IF LENGTH(@word) > 1 AND @word NOT IN ("the", "to", "and", "i", "was", "a", "in", "of", "on", "with", 
												"for", "is", "were", "", "singapore", "flight", "not", "my", 
                                                "that", "had", "it", "they", "we", "have", "but", "this", "from", 
                                                "me", "at", "no", "Airlines", "as", "very", "are", "airline", 
                                                "an", "be", "our", "We", "so", "which", "or", "their", "you", 
                                                "by", "would", "one", "only", "all", "when", "there", "us", 
                                                "before", "get", "airlines", "after") THEN
                INSERT INTO Q10b (Word) VALUES (@word);
            END IF;
        END WHILE;

        IF LENGTH(review_text) > 1 AND review_text NOT IN ("the", "to", "and", "i", "was", "a", "in", "of", 
												"on", "with", "for", "is", "were", "", "singapore", "flight", 
                                                "not", "my", "that", "had", "it", "they", "we", "have", "but", 
                                                "this", "from", "me", "at", "no", "Airlines", "as", "very", 
                                                "are", "airline", "an", "be", "our", "We", "so", "which", "or", 
                                                "their", "you", "by", "would", "one", "only", "all", "when", 
                                                "there", "us", "before", "get", "airlines", "after") THEN
            INSERT INTO Q10b (Word) VALUES (review_text);
        END IF;
    END LOOP;

    CLOSE cur;
END $$

DELIMITER ;

CALL extractwordsall();

/*
I run the below query to extract the top 10 words based on their frequency, using
GROUP BY, ORDER BY DESC, and LIMIT.
*/

SELECT Word, COUNT(Word) AS freq
FROM Q10b
GROUP BY Word
ORDER BY freq DESC
LIMIT 10;


/*
Now, I do a similar analysis to above, except that only reviews that have been verified will be considered.
*/

CREATE TEMPORARY TABLE Q10c
AS
SELECT LOWER(REPLACE(REPLACE(REPLACE(REPLACE(Reviews, '.', ''),',',''),'{',''),'}','')) AS ReviewText
FROM airlines_reviews
WHERE Airline = 'Singapore Airlines' 
    AND Recommended = 'no'
    AND Verified = "TRUE";

CREATE TEMPORARY TABLE Q10d (
    Word VARCHAR(50)
);

DELIMITER $$

/*
I create a new procedure that is similar to above, to break the content in the Reviews column of airlines_reviews into 
separate standalone words.
*/

CREATE PROCEDURE extractwordsverified()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE review_text VARCHAR(5000);
    DECLARE cur CURSOR FOR SELECT ReviewText FROM Q10c;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO review_text;
        IF done THEN
            LEAVE read_loop;
        END IF;

        WHILE LOCATE(' ', review_text) > 0 DO
            SET @word = SUBSTRING_INDEX(review_text, ' ', 1);
            SET review_text = SUBSTRING(review_text, LOCATE(' ', review_text) + 1);
            IF LENGTH(@word) > 1 AND @word NOT IN ("the", "to", "and", "i", "was", "a", "in", "of", "on", "with", 
												"for", "is", "were", "", "singapore", "flight", "not", "my", 
                                                "that", "had", "it", "they", "we", "have", "but", "this", "from", 
                                                "me", "at", "no", "Airlines", "as", "very", "are", "airline", 
                                                "an", "be", "our", "We", "so", "which", "or", "their", "you", 
                                                "by", "would", "one", "only", "all", "when", "there", "us", 
                                                "before", "get", "airlines", "after") THEN
                INSERT INTO Q10d (Word) VALUES (@word);
            END IF;
        END WHILE;

        IF LENGTH(review_text) > 1 AND review_text NOT IN ("the", "to", "and", "i", "was", "a", "in", "of", 
												"on", "with", "for", "is", "were", "", "singapore", "flight", 
                                                "not", "my", "that", "had", "it", "they", "we", "have", "but", 
                                                "this", "from", "me", "at", "no", "Airlines", "as", "very", 
                                                "are", "airline", "an", "be", "our", "We", "so", "which", "or", 
                                                "their", "you", "by", "would", "one", "only", "all", "when", 
                                                "there", "us", "before", "get", "airlines", "after") THEN
            INSERT INTO Q10d (Word) VALUES (review_text);
        END IF;
    END LOOP;

    CLOSE cur;
END $$

DELIMITER ;

CALL extractwordsverified();

/*
I run the below query to extract the top 10 words based on their frequency, using
GROUP BY, ORDER BY DESC, and LIMIT.
*/

SELECT Word, COUNT(Word) AS freq
FROM Q10d
GROUP BY Word
ORDER BY freq DESC
LIMIT 10;


/*
Next, I look for how general chatbots respond to different lexical variations.
I use the customer_support dataset for this. I note from the source of the dataset that there are mainly 2 types of 
lexical variations - morphological variation (inflectional and derivational), and semantic variations.
For each type of variation, I do the following:
I create 2 temporary tables. The first table will store the results of the SELECT query.
For this query, I remove briefly the common special characters that show up in the response column of customer_support.
Using the REPLACE function, I overwrite those special characters with no characters.
I filter the data that is relevant, by using the LIKE function to compare against what I need.
I use the flags column to help me extract out flags that contain M (for the morphological variation analysis), 
and flags that contain L (for the semantic variation analysis) respectively.
The next table is to create an empty table to store the outcome of my procedure.
*/

CREATE TEMPORARY TABLE Q10e
AS
SELECT LOWER(REPLACE(REPLACE(REPLACE(REPLACE(response, '.', ''),',',''),'{',''),'}','')) AS ResponseText
FROM customer_support
WHERE flags LIKE "%M%";

CREATE TEMPORARY TABLE Q10f (
    Word VARCHAR(50)
);

/*
I create a procedure to break the content in the response column of customer_support into separate standalone words.
I use a while loop to do this.
I indicated a set of words to exclude when storing the outcomes in the empty table.
Those words are common words that do not pose great importance or purpose in explaining any insights.
By excluding those words, I prevent the results from being diluted, and I will be able to focus on the
key words that chatbots use in responding to users.
I then use CALL to run the procedure.
*/

DELIMITER $$

CREATE PROCEDURE extractresponse1()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE response_text VARCHAR(5000);
    DECLARE cur CURSOR FOR SELECT ResponseText FROM Q10e;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO response_text;
        IF done THEN
            LEAVE read_loop;
        END IF;

        WHILE LOCATE(' ', response_text) > 0 DO
            SET @word = SUBSTRING_INDEX(response_text, ' ', 1);
            SET response_text = SUBSTRING(response_text, LOCATE(' ', response_text) + 1);
            IF LENGTH(@word) > 1 AND @word NOT IN ("to", "the", "you", "your", "with", "and", "for", "in", "i", 
												"our", "can", "or", "this", "that", "we", "any", "me", "will", 
                                                "of", "us", "here", "could", "i'm", "on", "need", "have", "is", 
                                                "would", "like", "you're", "a", "if", "are", "be", "may", "it", 
                                                "from", "as") THEN
                INSERT INTO Q10f (Word) VALUES (@word);
            END IF;
        END WHILE;

        IF LENGTH(response_text) > 1 AND response_text NOT IN ("to", "the", "you", "your", "with", "and", "for",
															"in", "i", "our", "can", "or", "this", "that", "we", 
                                                            "any", "me", "will", "of", "us", "here", "could", 
                                                            "i'm", "on", "need", "have", "is", "would", "like", 
                                                            "you're", "a", "if", "are", "be", "may", "it", 
                                                            "from", "as") THEN
            INSERT INTO Q10f (Word) VALUES (response_text);
        END IF;
    END LOOP;

    CLOSE cur;
END $$

DELIMITER ;

CALL extractresponse1();

/*
I run the below query to extract the top 20 words based on their frequency, using
GROUP BY, ORDER BY DESC, and LIMIT.
*/

SELECT Word, COUNT(Word) AS freq
FROM Q10f
GROUP BY Word
ORDER BY freq DESC
LIMIT 20;


/*
The same steps are applied for the semantic variations.
*/

CREATE TEMPORARY TABLE Q10g
AS
SELECT LOWER(REPLACE(REPLACE(REPLACE(REPLACE(response, '.', ''),',',''),'{',''),'}','')) AS ResponseText
FROM customer_support
WHERE flags LIKE "%L%";

CREATE TEMPORARY TABLE Q10h (
    Word VARCHAR(50)
);

DELIMITER $$

CREATE PROCEDURE extractresponse2()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE response_text VARCHAR(5000);
    DECLARE cur CURSOR FOR SELECT ResponseText FROM Q10g;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    OPEN cur;
    read_loop: LOOP
        FETCH cur INTO response_text;
        IF done THEN
            LEAVE read_loop;
        END IF;

        WHILE LOCATE(' ', response_text) > 0 DO
            SET @word = SUBSTRING_INDEX(response_text, ' ', 1);
            SET response_text = SUBSTRING(response_text, LOCATE(' ', response_text) + 1);
            IF LENGTH(@word) > 1 AND @word NOT IN ("to", "the", "you", "your", "with", "and", "for", "in", "i", 
												"our", "can", "or", "this", "that", "we", "any", "me", "will", 
                                                "of", "us", "here", "could", "i'm", "on", "need", "have", "is", 
                                                "would", "like", "you're", "a", "if", "are", "be", "may", "it", 
                                                "from", "as") THEN
                INSERT INTO Q10h (Word) VALUES (@word);
            END IF;
        END WHILE;

        IF LENGTH(response_text) > 1 AND response_text NOT IN ("to", "the", "you", "your", "with", "and", "for",
															"in", "i", "our", "can", "or", "this", "that", "we", 
                                                            "any", "me", "will", "of", "us", "here", "could", 
                                                            "i'm", "on", "need", "have", "is", "would", "like", 
                                                            "you're", "a", "if", "are", "be", "may", "it", 
                                                            "from", "as") THEN
            INSERT INTO Q10h (Word) VALUES (response_text);
        END IF;
    END LOOP;

    CLOSE cur;
END $$

DELIMITER ;

CALL extractresponse2();

SELECT Word, COUNT(Word) AS freq
FROM Q10h
GROUP BY Word
ORDER BY freq DESC
LIMIT 20;