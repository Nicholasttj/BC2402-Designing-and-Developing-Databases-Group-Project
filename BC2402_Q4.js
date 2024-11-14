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
