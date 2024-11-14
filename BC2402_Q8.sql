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