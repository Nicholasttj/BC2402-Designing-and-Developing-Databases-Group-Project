/* Q1 */ 

use "BC2402_GP"

db.customer_support.find()

db.customer_support.aggregate([
  {
    $group: {
      _id: "$category" 
    }
  },
  {
    $sort: { _id: 1 }  
  }
])

db.customer_support.aggregate([
  {
    $match: {
      category: { $ne: null, $ne: "" }  
    }
  },
  {
    $group: {
      _id: "$category"
    }
  },
  {
    $count: "number_of_categories_in_customer_support"
  }
])

db.customersupport.aggregate([
  {
    $match: {
      category: {
        $ne: null,
        $ne: "",
        $regex: /^[A-Z]+$/  
      }
    }
  },
  {
    $group: {
      _id: "$category"
    }
  },
  {
    $sort: { _id: 1 }  
  }
])

db.customer_support.aggregate([
  {
    $match: {
      category: {
        $ne: null,
        $ne: "",
        $regex: /^[A-Z]+$/  
      }
    }
  },
  {
    $group: {
      _id: "$category"
    }
  },
  {
    $count: "number of categories in customer support"
  }
])

/* 
Use database called "airlineproject" and retrieve and display all documents in the "customersupport" collection. 
Aggregate pipeline to group and sort unique categories. 
Group by unique "category" values
Sort the grouped categories in alphabet ascending order
Start counting non-null, non-empty "category"
Aggregate pipeline to count the number of non-null, non-empty categories
Group by unique "category" after filtering
Count total unique, non-null, non-empty "category" values and found 36 categories 
Start grouping and sorting categories that have values, are not empty, and only use capital letters
Skip rows where "category" is null, empty, or has lowercase letters. 
Count the distinct number of categories, which give me a result of 8 categories, after making sure it is of the distinct categories. 
*/ 

/*---------------------------------------------------------------------------------------------------------------------------------------------*/

/*Q2*/

/*For NOSQL, the process is silimar with SQL. Firstly, use the language tag to filter out the response to find 
colloquial variation and offensive language. 
Secondly, group them by category. */
db.customer_support.aggregate([
    {
        $match: {
            response: { $regex: 'offensive|colloquial|slang|abusive', $options: 'i' }
        }
    },
    /*This function filters the documents in the customer and Support collection to include only those whose 
    response fields contain any specified words*/
    {
        $group: {
            _id: "$category",
            records_with_colloquial_or_offensive_language: { $sum: 1 }
        }
    }
]);/*The filtered records are grouped by the category field and the number of records that 
meet the matching criteria in each group is calculated.*/

/*---------------------------------------------------------------------------------------------------------------------------------------------*/

/* 3 */

// First, I tally the number of delays. I filter out documents in which ArrDelay is more than 0.
// Then I group these documents by Airline. An increment of 1 is applied per document to the respective airline.
// Then I sort by the airline name which is _id.
// I do a merge for the results to a new collection called Q3.

db.flight_delay.aggregate([
    {$match:{"ArrDelay":{$gt:0}}},
    {$project:{"_id":0, "Airline":1, "ArrDelay":1}},
    {$group:{
        "_id":{"Airline":"$Airline"},
        "Number of delays":{$sum:1}}},
    {$sort:{"_id":1}},
    {$merge:{into:"Q3", on:"_id"}}
])

// Now I handle the number of cancellations. I implied from the data dictionary that the Cancelled column is a binary column,
// and 0 means Not Cancelled while 1 means Cancelled. So, I group each document by Airline, and sum up the Cancelled values
// of each document to the respective airline. This works as the 0 and 1 values can also be treated as the number of flights cancelled.
// For example, Cancelled = 0, means for that document 0 flights is cancelled. If Cancelled = 1, 1 flight is cancelled in that document.
// Then I sort by the airline name which is _id.
// I do a merge for the results to the collection Q3 created above.

db.flight_delay.aggregate([
    {$project:{"_id":0, "Airline":1, "Cancelled":1}},
    {$group:{
        "_id":{"Airline":"$Airline"},
        "Number of cancellations":{$sum:"$Cancelled"}}},
    {$sort:{"_id":1}},
    {$merge:{into:"Q3", on:"_id"}}
])

// I check the collection Q3 and find that the results from both queries are combined now, which is correct.

db.Q3.find()

// I set a threshold of ArrDelay more than 60 to be treated as a severe delay.
// I extract the number of instances per airline where the delay is severe.
// Then I calculate the proportion of severe delays over the total number of delays for each airline.

db.flight_delay.aggregate([
    {$match:{"ArrDelay":{$gt:60}}},
    {$project:{"_id":0, "Airline":1, "ArrDelay":1}},
    {$group:{
        "_id":{"Airline":"$Airline"},
        "Number of severe delays":{$sum:1}}},
    {$sort:{"_id":1}},
    {$merge:{into:"Q3a", on:"_id"}}
])

db.flight_delay.aggregate([
    {$match:{"ArrDelay":{$gt:0}}},
    {$project:{"_id":0, "Airline":1, "ArrDelay":1}},
    {$group:{
        "_id":{"Airline":"$Airline"},
        "Number of delays":{$sum:1}}},
    {$sort:{"_id":1}},
    {$merge:{into:"Q3a", on:"_id"}}
])

db.Q3a.aggregate([
    {$project:{"Number of severe delays":1, "Number of delays":1,
                "Percentage of severe delays":{$round:[{$multiply:[{$divide:["$Number of severe delays", "$Number of delays"]}, 100]},0]}}},
    {$sort:{"Percentage of severe delays":-1}}])
    
/*---------------------------------------------------------------------------------------------------------------------------------------------*/

/* 4 */

db.flight_delay.aggregate([

  // Step 1: Project to extract Month and define each Route
  {
    $project: {
      Month: {
        $month: {
          $dateFromString: {
            dateString: "$Date",
            format: "%d-%m-%Y" // Correct format for DD-MM-YYYY
          }
        }
      },
      Route: { 
        $concat: ["$Origin", "-", "$Dest"]  // Concatenate Origin and Dest to define each route
      },
      HasDelay: { $gt: ["$ArrDelay", 0] }  // Check if ArrDelay > 0 for delay instances
    }
  },

  // Step 2: Filter to include only records with delays
  { 
    $match: { HasDelay: true }  // Only include flights with ArrDelay > 0
  },

  // Step 3: Group by Month and Route to count the number of delay instances
  {
    $group: {
      _id: { Month: "$Month", Route: "$Route" },  // Group by month and route
      DelayCount: { $sum: 1 }  // Count instances of delays
    }
  },

  // Step 4: Sort by Month and DelayCount in descending order
  { 
    $sort: { "_id.Month": 1, "DelayCount": -1 }  // Sort by month and then by delay count in descending order
  },

  // Step 5: Group again by Month to get the route with the most delays
  {
    $group: {
      _id: "$_id.Month",  // Group by month
      Route: { $first: "$_id.Route" },  // Select the route with the highest delay count for each month
      MaxDelayCount: { $first: "$DelayCount" }  // Select the highest delay count for each month
    }
  },

  // Step 6: Sort by Month to display the results in chronological order
  {
    $sort: { "_id": 1 }  // Sort by month in ascending order
  }
]);

/*---------------------------------------------------------------------------------------------------------------------------------------------*/

/* 5 */

db.sia_stock.aggregate([
    /* Step 1: Filter for records in 2023 */
    {
        $match: {
            StockDate: { $regex: /2023$/ }
        }
    },
    /* Step 2: Extract Quarter from StockDate */
    {
        $project: {
            quarter: {
                $switch: {
                    branches: [
                        { case: { $in: [ { $substr: ["$StockDate", 0, 2] }, ["01", "02", "03"] ] }, then: 1 },
                        { case: { $in: [ { $substr: ["$StockDate", 0, 2] }, ["04", "05", "06"] ] }, then: 2 },
                        { case: { $in: [ { $substr: ["$StockDate", 0, 2] }, ["07", "08", "09"] ] }, then: 3 },
                        { case: { $in: [ { $substr: ["$StockDate", 0, 2] }, ["10", "11", "12"] ] }, then: 4 }
                    ],
                    default: null
                }
            },
            High: 1,
            Low: 1,
            Price: 1
        }
    },
    /* Step 3: Group by quarter to calculate highest, lowest, and average prices */
    {
        $group: {
            _id: "$quarter",
            max_high: { $max: "$High" },
            min_low: { $min: "$Low" },
            avg_price: { $avg: "$Price" }
        }
    },
    /* Step 4: Sort by quarter in ascending order */
    {
        $sort: { _id: 1 }
    },
    /* Step 5: Calculate QoQ changes */
    {
        $setWindowFields: {
            sortBy: { _id: 1 },
            output: {
                prev_max_high: { $shift: { output: "$max_high", by: -1 } },
                prev_min_low: { $shift: { output: "$min_low", by: -1 } },
                prev_avg_price: { $shift: { output: "$avg_price", by: -1 } }
            }
        }
    },
    /* Step 6: QoQ changes as percentage */
    {
        $project: {
            quarter: "$_id",
            current_max_high: "$max_high",
            current_min_low: "$min_low",
            current_avg_price: { $round: ["$avg_price", 2] }, // Rounded to 2 decimal places
            qoq_high_change: {
                $cond: {
                    if: { $ne: ["$prev_max_high", null] },
                    then: { 
                        $concat: [
                            { $toString: { $round: [{ $multiply: [{ $divide: [{ $subtract: ["$max_high", "$prev_max_high"] }, "$prev_max_high"] }, 100] }, 2] } },
                            "%"
                        ]
                    },
                    else: null
                }
            },
            qoq_low_change: {
                $cond: {
                    if: { $ne: ["$prev_min_low", null] },
                    then: { 
                        $concat: [
                            { $toString: { $round: [{ $multiply: [{ $divide: [{ $subtract: ["$min_low", "$prev_min_low"] }, "$prev_min_low"] }, 100] }, 2] } },
                            "%"
                        ]
                    },
                    else: null
                }
            },
            qoq_avg_price_change: {
                $cond: {
                    if: { $ne: ["$prev_avg_price", null] },
                    then: { 
                        $concat: [
                            { $toString: { $round: [{ $multiply: [{ $divide: [{ $subtract: ["$avg_price", "$prev_avg_price"] }, "$prev_avg_price"] }, 100] }, 2] } },
                            "%"
                        ]
                    },
                    else: null
                }
            }
        }
    }
]);

/* # The result for Quarter 1 on QOQ changes will display as NULL because we are comparing the quarters only in the year of 2023. 
# Hence, Quarters 2,3,4 will be able to compare its results to the previous quarter, but quarter 1 is the first quarter of the year and do not have previous quarter to compare against.
# Also, the QOQ changes shows the calculated percentage change compared to the previous quarter.

# Findings 1: Q2 saw a general increase across the board, with significant gains in both the highest and average prices. 
# This could suggest increased market activity or demand during this period.

# Findings 2: For Q3, the average price continued to rise despite a dip in the highest price, suggesting more stability in pricing with fewer extreme highs. 
# The rise in the lowest price indicates that the overall price range shifted upwards. 

# Findings 3: Q4 saw a downward trend in prices across all metrics. The declines in both high and low prices suggest reduced market activity or 
# demand in the final quarter of the year.

# This QOQ analysis suggests fluctuations in prices, with a peak around Q2 and a gradual decline towards the end of the year. 
# This pattern could be seasonal or reflect broader market trends impacting prices throughout the year. */

/*---------------------------------------------------------------------------------------------------------------------------------------------*/

/* 6 */

db.customer_booking.aggregate([
    // First stage: Grouping data by sales_channel and route
    {
        $group: {
            _id: { 
                // Group by 'sales_channel' and 'route'
                sales_channel: "$sales_channel",  
                route: "$route"  
            },
            // Compute the average of 'length_of_stay' for each group
            avg_length_of_stay: { $avg: "$length_of_stay" },
            // Compute the average of 'flight_duration' for each group
            avg_flight_duration: { $avg: "$flight_duration" },
            // Compute the average of 'wants_extra_baggage' for each group
            avg_wants_extra_baggage: { $avg: "$wants_extra_baggage" },
            // Compute the average of 'wants_preferred_seat' for each group
            avg_wants_preferred_seat: { $avg: "$wants_preferred_seat" },
            // Compute the average of 'wants_in_flight_meals' for each group
            avg_wants_in_flight_meals: { $avg: "$wants_in_flight_meals" }
        }
    },
    // Second stage: Projecting new fields based on computed averages
    {
        $project: {
            sales_channel: "$_id.sales_channel",
            route: "$_id.route",
            // Calculate average length of stay per flight duration (if flight duration is 0, return null to avoid division by zero)
            avg_length_of_stay_per_flight_hour: {
                $cond: {
                    if: { $eq: ["$avg_flight_duration", 0] },  // Check if 'avg_flight_duration' is 0
                    then: null,  // If true, return null
                    else: { $divide: ["$avg_length_of_stay", "$avg_flight_duration"] }  // Otherwise, divide 'avg_length_of_stay' by 'avg_flight_duration'
                }
            },
            // Calculate average wants for extra baggage per flight duration (handle zero flight duration similarly)
            avg_wants_extra_baggage_per_flight_hour: {
                $cond: {
                    if: { $eq: ["$avg_flight_duration", 0] },
                    then: null,
                    else: { $divide: ["$avg_wants_extra_baggage", "$avg_flight_duration"] }
                }
            },
            // Calculate average wants for preferred seat per flight duration (handle zero flight duration similarly)
            avg_wants_preferred_seat_per_flight_hour: {
                $cond: {
                    if: { $eq: ["$avg_flight_duration", 0] },
                    then: null,
                    else: { $divide: ["$avg_wants_preferred_seat", "$avg_flight_duration"] }
                }
            },
            // Calculate average wants for in-flight meals per flight duration (handle zero flight duration similarly)
            avg_wants_in_flight_meals_per_flight_hour: {
                $cond: {
                    if: { $eq: ["$avg_flight_duration", 0] },
                    then: null,
                    else: { $divide: ["$avg_wants_in_flight_meals", "$avg_flight_duration"] }
                }
            }
        }
    }
]);

/*---------------------------------------------------------------------------------------------------------------------------------------------*/

/* 7 */

/* Seasonal period (June to September) */
db.airlines_reviews.aggregate([
    {
        $match: {
            MonthFlown: { $in: ["Jun-23", "Jul-23", "Aug-23", "Sep-23", "Jun-24", "Jul-24", "Aug-24", "Sep-24"] }
        }
    },
    {
        $group: {
            _id: {
                airline: "$Airline",
                class: "$Class",
                season: "Seasonal"
            },
            avg_seat_comfort: { $avg: "$SeatComfort" },
            avg_food_beverages: { $avg: "$FoodnBeverages" },
            avg_inflight_entertainment: { $avg: "$InflightEntertainment" },
            avg_value_for_money: { $avg: "$ValueForMoney" },
            avg_overall_rating: { $avg: "$OverallRating" }
        }
    },
    {
        $project: {
            airline: "$_id.airline",
            class: "$_id.class",
            season: "$_id.season",
            avg_seat_comfort: { $round: ["$avg_seat_comfort", 2] },
            avg_food_beverages: { $round: ["$avg_food_beverages", 2] },
            avg_inflight_entertainment: { $round: ["$avg_inflight_entertainment", 2] },
            avg_value_for_money: { $round: ["$avg_value_for_money", 2] },
            avg_overall_rating: { $round: ["$avg_overall_rating", 2] }
        }
    }
]);

/* Non-seasonal period (all other months) */
db.airlines_reviews.aggregate([
    {
        $match: {
            MonthFlown: { $nin: ["Jun-23", "Jul-23", "Aug-23", "Sep-23", "Jun-24", "Jul-24", "Aug-24", "Sep-24"] }
        }
    },
    {
        $group: {
            _id: {
                airline: "$Airline",
                class: "$Class",
                season: "Non-Seasonal"
            },
            avg_seat_comfort: { $avg: "$SeatComfort" },
            avg_food_beverages: { $avg: "$FoodnBeverages" },
            avg_inflight_entertainment: { $avg: "$InflightEntertainment" },
            avg_value_for_money: { $avg: "$ValueForMoney" },
            avg_overall_rating: { $avg: "$OverallRating" }
        }
    },
    {
        $project: {
            airline: "$_id.airline",
            class: "$_id.class",
            season: "$_id.season",
            avg_seat_comfort: { $round: ["$avg_seat_comfort", 2] },
            avg_food_beverages: { $round: ["$avg_food_beverages", 2] },
            avg_inflight_entertainment: { $round: ["$avg_inflight_entertainment", 2] },
            avg_value_for_money: { $round: ["$avg_value_for_money", 2] },
            avg_overall_rating: { $round: ["$avg_overall_rating", 2] }
        }
    }
]);

/*---------------------------------------------------------------------------------------------------------------------------------------------*/

/* 8 */

db.airlines_reviews.aggregate([
  // Step 1: Categorize complaints based on keywords in the Reviews
  {
    $addFields: {
      ComplaintType: {
        $switch: {
          branches: [
            { case: { $regexMatch: { input: "$Reviews", regex: /delay/i } }, then: "Delay" },
            { case: { $regexMatch: { input: "$Reviews", regex: /lost baggage|luggage/i } }, then: "Lost Baggage" },
            { case: { $regexMatch: { input: "$Reviews", regex: /rude|unfriendly/i } }, then: "Rude Staff" },
            { case: { $regexMatch: { input: "$Reviews", regex: /legroom|cramped/i } }, then: "Lack of Comfort" },
            { case: { $regexMatch: { input: "$Reviews", regex: /food|meal/i } }, then: "Poor Food Quality" },
          ],
          default: "Other"
        }
      }
    }
  },

  // Step 2: Count occurrences of each complaint type per airline and type of traveller
  {
    $group: {
      _id: {
        Airline: "$Airline",
        TypeofTraveller: "$TypeofTraveller",
        ComplaintType: "$ComplaintType"
      },
      Complaint_Frequency: { $sum: 1 }  // Count occurrences of each complaint type
    }
  },

  // Step 3: Rank complaints based on frequency per airline and type of traveller
  {
    $sort: { "_id.Airline": 1, "_id.TypeofTraveller": 1, "Complaint_Frequency": -1 }  // Sort by airline, traveller type, and frequency in descending order
  },
  {
    $setWindowFields: {
      // Combine the fields into a single string key for partitionings
      partitionBy: {
        $concat: ["$_id.Airline", "_", "$_id.TypeofTraveller"]
      },  // Partition by combined Airline and TypeofTraveller fields
      sortBy: { Complaint_Frequency: -1 },  // Sort by complaint frequency in descending order
      output: {
        Ranking: { $rank: {} }  // Assign ranks to complaints based on frequency
      }
    }
  },

  // Step 4: Select top 5 complaints for each airline and type of traveller
  {
    $match: { Ranking: { $lte: 5 } }  // Keep only the top 5 complaints per airline and traveller type
  },

  // Step 5: Format the output with top complaints
  {
    $group: {
      _id: {
        Airline: "$_id.Airline",
        TypeofTraveller: "$_id.TypeofTraveller"
      },
      Complaints: { 
        $push: {
          ComplaintType: "$_id.ComplaintType",
          Complaint_Frequency: "$Complaint_Frequency",
          Ranking: "$Ranking"
        }
      }
    }
  },
  {
    $project: {
      _id: 1,
      Top_Complaint_1: { $arrayElemAt: ["$Complaints.ComplaintType", 0] },
      Complaint_Count_1: { $arrayElemAt: ["$Complaints.Complaint_Frequency", 0] },

      Top_Complaint_2: { $arrayElemAt: ["$Complaints.ComplaintType", 1] },
      Complaint_Count_2: { $arrayElemAt: ["$Complaints.Complaint_Frequency", 1] },

      Top_Complaint_3: { $arrayElemAt: ["$Complaints.ComplaintType", 2] },
      Complaint_Count_3: { $arrayElemAt: ["$Complaints.Complaint_Frequency", 2] },

      Top_Complaint_4: { $arrayElemAt: ["$Complaints.ComplaintType", 3] },
      Complaint_Count_4: { $arrayElemAt: ["$Complaints.Complaint_Frequency", 3] },

      Top_Complaint_5: { $arrayElemAt: ["$Complaints.ComplaintType", 4] },
      Complaint_Count_5: { $arrayElemAt: ["$Complaints.Complaint_Frequency", 4] }
    }
  },

  // Step 6: Sort the results for better presentation
  {
    $sort: { "_id.Airline": 1, "_id.TypeofTraveller": 1 }
  }
]);

/*---------------------------------------------------------------------------------------------------------------------------------------------*/

/* 9 */

/* Recommended with Individual Ratings */

db.airlines_reviews.aggregate([
    {
        $match: { Airline: "Singapore Airlines" }
    },
    {
        $addFields: {
            Period: {
                $switch: {
                    branches: [
                        { 
                            case: { $lt: [ { $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2020-03-11") ] },
                            then: "Pre-COVID" 
                        },
                        { 
                            case: { $and: [
                                { $gte: [ { $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2020-03-11") ] },
                                { $lte: [ { $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2022-04-04") ] }
                            ]},
                            then: "Peri-COVID" 
                        }
                    ],
                    default: "Post-COVID"
                }
            }
        }
    },
    {
        $group: {
            _id: "$Period",
            AvgRating: { $avg: "$OverallRating" },
            NumRecommended: { $sum: { $cond: [{ $eq: [{ $toLower: "$Recommended" }, "yes"] }, 1, 0] } },
            TotalReviews: { $sum: 1 },
            ServiceRelatedCount: { $sum: { $cond: [{ $gte: ["$StaffService", 3] }, 1, 0] } },
            PriceRelatedCount: { $sum: { $cond: [{ $gte: ["$ValueForMoney", 3] }, 1, 0] } },
            SeatRelatedCount: { $sum: { $cond: [{ $gte: ["$SeatComfort", 3] }, 1, 0] } },
            FoodRelatedCount: { $sum: { $cond: [{ $gte: ["$FoodnBeverages", 3] }, 1, 0] } }
        }
    },
    {
        $addFields: {
            AvgRating: { $round: ["$AvgRating", 2] }, 
            PercentageRecommended: {
                $concat: [
                    { $toString: { $round: [{ $multiply: [{ $divide: ["$NumRecommended", "$TotalReviews"] }, 100] }, 2] } },
                    "%"
                ]
            },
            ServiceRelated: {
                $concat: [
                    { $toString: { $round: [{ $multiply: [{ $divide: ["$ServiceRelatedCount", "$TotalReviews"] }, 100] }, 2] } },
                    "%"
                ]
            },
            PriceRelated: {
                $concat: [
                    { $toString: { $round: [{ $multiply: [{ $divide: ["$PriceRelatedCount", "$TotalReviews"] }, 100] }, 2] } },
                    "%"
                ]
            },
            SeatRelated: {
                $concat: [
                    { $toString: { $round: [{ $multiply: [{ $divide: ["$SeatRelatedCount", "$TotalReviews"] }, 100]  }, 2] } },
                    "%"
                ]
            },
            FoodRelated: {
                $concat: [
                    { $toString: { $round: [{ $multiply: [{ $divide: ["$FoodRelatedCount", "$TotalReviews"] }, 100] }, 2] } },
                    "%"
                ]
            }
        }
    },
    {
        $project: {
            Period: "$_id",
            AvgRating: 1,
            NumRecommended: 1,
            TotalReviews: 1,
            PercentageRecommended: 1,
            ServiceRelated: 1,
            PriceRelated: 1,
            SeatRelated: 1,
            FoodRelated: 1
        }
    }
]);

/* 
Use database called "airline" 
Filter documents to only include reviews for "Singapore Airlines". 
Categorize each review by ISO date into Pre-COVID (2020-03-11), Peri-COVID (between '2020-03-11' and '2022-04-04'), or Post-COVID periods (as long as it is not within the Pre-COVID and Peri COVID). 
Group the reviews by the "Period" field and calculate aggregated values for each period. 
Round the average rating for each period. 
Count the number of reviews where the recommendation is 'yes', and the total number of reviews for each period.
Then calculate the percentage of reviews for StaffService, ValueForMoney, SeatComfort, FoodnBeverages that is above and equal 3. 
Rename _id field to Period. 
*/ 


/* Not Recommended with Individual Ratings */
db.airlines_reviews.aggregate([
    {
        $match: { Airline: "Singapore Airlines" }
    },
    {
        $addFields: {
            Period: {
                $switch: {
                    branches: [
                        { case: { $lt: [ { $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2020-03-11") ] }, then: "Pre-COVID" },
                        { case: { $and: [ { $gte: [ { $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2020-03-11") ] }, { $lte: [ { $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2022-04-04") ] } ]}, then: "Peri-COVID" }
                    ],
                    default: "Post-COVID"
                }
            }
        }
    },
    {
        $group: {
            _id: "$Period",
            AvgRating: { $avg: "$OverallRating" },
            NumNotRecommended: { $sum: { $cond: [{ $eq: [{ $toLower: "$Recommended" }, "no"] }, 1, 0] } },
            TotalReviews: { $sum: 1 },
            ServiceRelatedCount: { $sum: { $cond: [{ $lt: ["$StaffService", 3] }, 1, 0] } },
            PriceRelatedCount: { $sum: { $cond: [{ $lt: ["$ValueForMoney", 3] }, 1, 0] } },
            SeatRelatedCount: { $sum: { $cond: [{ $lt: ["$SeatComfort", 3] }, 1, 0] } },
            FoodRelatedCount: { $sum: { $cond: [{ $lt: ["$FoodnBeverages", 3] }, 1, 0] } }
        }
    },
    {
        $addFields: {
            AvgRating: { $round: ["$AvgRating", 2] }, 
            PercentageNotRecommended: {
                $concat: [
                    { $toString: { $round: [{ $multiply: [{ $divide: ["$NumNotRecommended", "$TotalReviews"] }, 100] }, 2] } },
                    "%"
                ]
            },
            ServiceRelated: {
                $concat: [
                    { $toString: { $round: [{ $multiply: [{ $divide: ["$ServiceRelatedCount", "$TotalReviews"] }, 100] }, 2] } },
                    "%"
                ]
            },
            PriceRelated: {
                $concat: [
                    { $toString: { $round: [{ $multiply: [{ $divide: ["$PriceRelatedCount", "$TotalReviews"] }, 100] }, 2] } },
                    "%"
                ]
            },
            SeatRelated: {
                $concat: [
                    { $toString: { $round: [{ $multiply: [{ $divide: ["$SeatRelatedCount", "$TotalReviews"] }, 100] }, 2] } },
                    "%"
                ]
            },
            FoodRelated: {
                $concat: [
                    { $toString: { $round: [{ $multiply: [{ $divide: ["$FoodRelatedCount", "$TotalReviews"] }, 100] }, 2] } },
                    "%"
                ]
            }
        }
    },
    {
        $project: {
            Period: "$_id",
            AvgRating: 1,
            NumNotRecommended: 1,
            TotalReviews: 1,
            PercentageNotRecommended: 1,
            ServiceRelated: 1,
            PriceRelated: 1,
            SeatRelated: 1,
            FoodRelated: 1
        }
    }
]);

/* 
Use database called "airline" 
Filter documents to only include reviews for "Singapore Airlines". 
Categorize each review by ISO date into Pre-COVID (2020-03-11), Peri-COVID (between '2020-03-11' and '2022-04-04'), or Post-COVID periods (as long as it is not within the Pre-COVID and Peri COVID). 
Group the reviews by the "Period" field and calculate aggregated values for each period. 
Round the average rating for each period. 
Count the number of reviews where the recommendation is 'no', and the total number of reviews for each period.
Then calculate the percentage of reviews for StaffService, ValueForMoney, SeatComfort, FoodnBeverages that is below 3. 
Rename _id field to Period. 
*/ 

/* Common Words across All Recommended Reviews */
const reviewsComparison = db.airlines_reviews.aggregate([
    {
        $match: { Airline: "Singapore Airlines" }
    },
    {
        $addFields: {
            Period: {
                $switch: {
                    branches: [
                        { 
                            case: { $lt: [ { $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2020-03-11") ] },
                            then: "Pre-COVID" 
                        },
                        { 
                            case: { $and: [
                                { $gte: [ { $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2020-03-11") ] },
                                { $lte: [ { $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2022-04-04") ] }
                            ]},
                            then: "Peri-COVID" 
                        }
                    ],
                    default: "Post-COVID"
                }
            }
        }
    },
    {
        $match: { Recommended: "yes" } 
    },
    {
        $group: {
            _id: "$Period",
            Reviews: { $push: "$Reviews" }  
        }
    }
]).toArray();  

let preCovidReviews = (reviewsComparison.find(r => r._id === "Pre-COVID") || {}).Reviews || [];
let periCovidReviews = (reviewsComparison.find(r => r._id === "Peri-COVID") || {}).Reviews || [];
let postCovidReviews = (reviewsComparison.find(r => r._id === "Post-COVID") || {}).Reviews || [];

const allReviews = {
    "Pre-COVID": preCovidReviews,
    "Peri-COVID": periCovidReviews,
    "Post-COVID": postCovidReviews
};

const stopWords = new Set([
    "i", "and", "was", "to", "a", "in", "of", "on", "with", "for", "is", "were",
    "my", "it", "not", "we", "that", "but", "this", "they", "very",
    "as", "singapore", "airlines", "from", "had", "flight", "at", "have", "no",
    "an", "be", "t", "so", "the", "me", "you", "our", "are", "by", "only","good","when"
]);

function compareAllReviews(reviewsObject) {
    let wordCount = {
        "Pre-COVID": {},
        "Peri-COVID": {},
        "Post-COVID": {}
    };

    for (const [period, reviews] of Object.entries(reviewsObject)) {
        reviews.forEach(review => {
            let words = review.toLowerCase().split(/\W+/);
            words.forEach(word => {
                if (word && !stopWords.has(word)) {  
                    wordCount[period][word] = (wordCount[period][word] || 0) + 1;
                }
            });
        });
    }

    let commonWords = {};
    for (const period in wordCount) {
        for (const word in wordCount[period]) {
            commonWords[word] = commonWords[word] || {};
            commonWords[word][period] = wordCount[period][word];
        }
    }

    const sortedResults = Object.entries(commonWords)
        .map(([word, counts]) => {
            return { word, counts };  
        })
        .sort((a, b) => {
           
            const totalA = Object.values(a.counts).reduce((sum, count) => sum + count, 0);
            const totalB = Object.values(b.counts).reduce((sum, count) => sum + count, 0);
            return totalB - totalA; 
        });

    return sortedResults;
}

let comparisonAllPeriods = compareAllReviews(allReviews);
console.log("Common Words across All Recommended Reviews:", comparisonAllPeriods);

/* 
Use database called "airline" 
Filter documents to only include reviews for "Singapore Airlines". 
Categorize each review by ISO date into Pre-COVID (2020-03-11), Peri-COVID (between '2020-03-11' and '2022-04-04'), or Post-COVID periods (as long as it is not within the Pre-COVID and Peri COVID). 
Filter for reviews only where recommendation is "yes"
Group reviews by period, gather all review texts in "Reviews" array
Make aggregation result into array so JavaScript can use easy
Pull out reviews by period into different variables, if it is not empty array
Put reviews by period all together into one object for check
Make a list of stop words to leave out from word count
Function to look at word count across all review period 
Set up word count object for each period
Go through each period, and each review, count how many times words show up
Split review into words, change to lowercase, leave out stop words
Add one to word count for each word in the current period
Initialize an object to store common words across periods and count each word by period
Convert word counts into array, sort by most times word comes up in all periods, and make each word and count into new format
Return the sorted array of word counts
Run the word frequency comparison for all review periods and print the results 
*/ 

/* Common Words across All Not Recommended Reviews */
const reviewsComparison = db.airlines_reviews.aggregate([
    {
        $match: { Airline: "Singapore Airlines" }
    },
    {
        $addFields: {
            Period: {
                $switch: {
                    branches: [
                        { 
                            case: { $lt: [ { $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2020-03-11") ] },
                            then: "Pre-COVID" 
                        },
                        { 
                            case: { $and: [
                                { $gte: [ { $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2020-03-11") ] },
                                { $lte: [ { $dateFromString: { dateString: "$ReviewDate", format: "%d/%m/%Y" } }, ISODate("2022-04-04") ] }
                            ]},
                            then: "Peri-COVID" 
                        }
                    ],
                    default: "Post-COVID"
                }
            }
        }
    },
    {
        $match: { Recommended: "no" } 
    },
    {
        $group: {
            _id: "$Period",
            Reviews: { $push: "$Reviews" }  
        }
    }
]).toArray();  

let preCovidReviews = (reviewsComparison.find(r => r._id === "Pre-COVID") || {}).Reviews || [];
let periCovidReviews = (reviewsComparison.find(r => r._id === "Peri-COVID") || {}).Reviews || [];
let postCovidReviews = (reviewsComparison.find(r => r._id === "Post-COVID") || {}).Reviews || [];

const allReviews = {
    "Pre-COVID": preCovidReviews,
    "Peri-COVID": periCovidReviews,
    "Post-COVID": postCovidReviews
};

const stopWords = new Set([
    "i", "and", "was", "to", "a", "in", "of", "on", "with", "for", "is", "were",
    "my", "it", "not", "we", "that", "but", "this", "they", "very",
    "as", "singapore", "airlines", "from", "had", "flight", "at", "have", "no",
    "an", "be", "t", "so", "the", "me", "you", "our", "are", "by", "only","good","when"
]);

function compareAllReviews(reviewsObject) {
    let wordCount = {
        "Pre-COVID": {},
        "Peri-COVID": {},
        "Post-COVID": {}
    };

    for (const [period, reviews] of Object.entries(reviewsObject)) {
        reviews.forEach(review => {
            let words = review.toLowerCase().split(/\W+/);
            words.forEach(word => {
                if (word && !stopWords.has(word)) { 
                    wordCount[period][word] = (wordCount[period][word] || 0) + 1;
                }
            });
        });
    }

    let commonWords = {};
    for (const period in wordCount) {
        for (const word in wordCount[period]) {
            commonWords[word] = commonWords[word] || {};
            commonWords[word][period] = wordCount[period][word];
        }
    }

    const sortedResults = Object.entries(commonWords)
        .map(([word, counts]) => {
            return { word, counts }; 
        })
        .sort((a, b) => {
           
            const totalA = Object.values(a.counts).reduce((sum, count) => sum + count, 0);
            const totalB = Object.values(b.counts).reduce((sum, count) => sum + count, 0);
            return totalB - totalA; 
        });

    return sortedResults;
}

let comparisonAllPeriods = compareAllReviews(allReviews);
console.log("Common Words across All Not Recommended Reviews:", comparisonAllPeriods);

/* 
Use database called "airline" 
Filter documents to only include reviews for "Singapore Airlines". 
Categorize each review by ISO date into Pre-COVID (2020-03-11), Peri-COVID (between '2020-03-11' and '2022-04-04'), or Post-COVID periods (as long as it is not within the Pre-COVID and Peri COVID). 
Filter for reviews only where recommendation is "no"
Group reviews by period, gather all review texts in "Reviews" array
Make aggregation result into array so JavaScript can use easy
Pull out reviews by period into different variables, if it is not empty array
Put reviews by period all together into one object for check
Make a list of stop words to leave out from word count
Function to look at word count across all review period 
Set up word count object for each period
Go through each period, and each review, count how many times words show up
Split review into words, change to lowercase, leave out stop words
Add one to word count for each word in the current period
Initialize an object to store common words across periods and count each word by period
Convert word counts into array, sort by most times word comes up in all periods, and make each word and count into new format
Return the sorted array of word counts
Run the word frequency comparison for all review periods and print the results 
*/ 

/*---------------------------------------------------------------------------------------------------------------------------------------------*/

/* 10 */

// Firstly, I extract the relevant issues based on the reviews of travellers from airlines_reviews.
// I do a match to filter out reviews for Singapore Airlines and those who do not recommend the airline.
// This is because it is less likely for complaints to pop up in reviews that recommend Singapore Airlines.
// I note also that in the dataset, there is a column to indicate if the review is verified.
// Hence, I split the analysis into 2 parts to see if there is any difference in the results when the unverified
// reviews are excluded. In this part, I include reviews that are both verified and not verified.
// I split the reviews into individual words and remove special characters.
// Next I remove stop words from the results to prevent dilution of the data.
// I do a grouping and obtain the frequency of each word, and display the top 10 words in the reviews.
// The 10 words may give insights into the key issues of Singapore Airlines that travellers face.

db.airlines_reviews.aggregate([
    {$match:{"Airline":"Singapore Airlines"}},
    {$match:{"Recommended":"no"}},
    {$project:{"words":{$split:["$Reviews"," "]},qty:1}},
    {$unwind: "$words"},
    {$project:{"words":{$trim:{input:"$words", chars:"!@#$%^&*()<>?,./-_+=[]{}:;\n"}}}},
    {$project:{"words":{$toLower:"$words"}}},
    {$match:{"words":{$nin:["the", "to", "and", "i", "was", "a", "in", "of", "on", "with", "for", "is", "were",
                            "", "singapore", "flight", "not", "my", "that", "had", "it", "they", "we", "have", 
                            "but", "this", "from", "me", "at", "no", "Airlines", "as", "very", "are", "airline", 
                            "an", "be", "our", "We", "so", "which", "or", "their", "you", "by", "would", "one", 
                            "only", "all", "when", "there", "us", "before", "get", "airlines", "after"]}}},
    {$group: {"_id":{"word":"$words"}, "frequency":{$sum:1}}},
    {$sort:{"frequency":-1}},
    {$limit:10}
])

// In this query, I perform similar analysis as the above query.
// There is an additional condition to include only verified reviews that are related to Singapore Airlines
// and are not recommended.

db.airlines_reviews.aggregate([
    {$match:{"Airline":"Singapore Airlines"}},
    {$match:{"Recommended":"no"}},
    {$match:{"Verified":"TRUE"}},
    {$project:{"words":{$split:["$Reviews"," "]},qty:1}},
    {$unwind: "$words"},
    {$project:{"words":{$trim:{input:"$words", chars:"!@#$%^&*()<>?,./-_+=[]{}:;\n"}}}},
    {$project:{"words":{$toLower:"$words"}}},
    {$match:{"words":{$nin:["the", "to", "and", "i", "was", "a", "in", "of", "on", "with", "for", "is", "were",
                            "", "singapore", "flight", "not", "my", "that", "had", "it", "they", "we", "have", 
                            "but", "this", "from", "me", "at", "no", "Airlines", "as", "very", "are", "airline", 
                            "an", "be", "our", "We", "so", "which", "or", "their", "you", "by", "would", "one", 
                            "only", "all", "when", "there", "us", "before", "get", "airlines", "after"]}}},
    {$group: {"_id":{"word":"$words"}, "frequency":{$sum:1}}},
    {$sort:{"frequency":-1}},
    {$limit:10}
])

// Next, general chatbot responses to different lexical variations are considered using customer_support.
// From the dataset source, there are 2 main types of lexical variations - morphological and semantic.
// Hence, I create 2 queries to extract each lexical variation and find the 20 most commonly found words
// in the chatbot's responses. The first query is to extract morphological variations from the flags column.
// I first convert all words to lower so that the frequency is more accurately computed.
// Similar to the above queries, I split the responses into individual words, and remove special characters.
// I also remove common stop words so as to not dilute the results.

db.customer_support.aggregate([
    {$project:{"_id":0, "flags":{$toLower:"$flags"}, "response":1}}
    {$match:{"flags":/m/}},
    {$project:{"words":{$split:["$response"," "]},qty:1}},
    {$unwind: "$words"},
    {$project:{"words":{$trim:{input:"$words", chars:"!@#$%^&*()<>?,./-_+=[]{}:;\n"}}}},
    {$project:{"words":{$toLower:"$words"}}},
    {$match:{"words":{$nin:["to", "the", "you", "your", "with", "and", "for", "in", "i", "our", "can", "or", 
                            "this", "that", "we", "any", "me", "will", "of", "us", "here", "could", "i'm", 
                            "on", "need", "have", "is", "would", "like", "you're", "a", "if", "are", "be", 
                            "may", "it", "from", "as"]}}},
    {$group: {"_id":{"word":"$words"}, "frequency":{$sum:1}}},
    {$sort:{"frequency":-1}},
    {$limit:20}
])

// This query is to extract semantic variations from the flags column.
// The functions and methodology used is similar to the above query.

db.customer_support.aggregate([
    {$project:{"_id":0, "flags":{$toLower:"$flags"}, "response":1}}
    {$match:{"flags":/l/}},
    {$project:{"words":{$split:["$response"," "]},qty:1}},
    {$unwind: "$words"},
    {$project:{"words":{$trim:{input:"$words", chars:"!@#$%^&*()<>?,./-_+=[]{}:;\n"}}}},
    {$project:{"words":{$toLower:"$words"}}},
    {$match:{"words":{$nin:["to", "the", "you", "your", "with", "and", "for", "in", "i", "our", "can", "or", 
                            "this", "that", "we", "any", "me", "will", "of", "us", "here", "could", "i'm", 
                            "on", "need", "have", "is", "would", "like", "you're", "a", "if", "are", "be", 
                            "may", "it", "from", "as"]}}},
    {$group: {"_id":{"word":"$words"}, "frequency":{$sum:1}}},
    {$sort:{"frequency":-1}},
    {$limit:20}
])