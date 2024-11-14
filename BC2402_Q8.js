db.airlines_reviews.aggregate([
  {
    $addFields: {
      ComplaintType: {
        $switch: {
          branches: [
            { case: { $regexMatch: { input: "$Reviews", regex: /delay/i } }, then: "Delay" },
            { case: { $regexMatch: { input: "$Reviews", regex: /lost baggage|luggage/i } }, then: "Lost Baggage" },
            { case: { $regexMatch: { input: "$Reviews", regex: /rude|unfriendly/i } }, then: "Rude Staff" },
            { case: { $regexMatch: { input: "$Reviews", regex: /legroom|cramped/i } }, then: "Lack of Comfort" },
            { case: { $regexMatch: { input: "$Reviews", regex: /food|meal/i } }, then: "Poor Food Quality" }
          ],
          default: "Other"
        }
      }
    }
  }
])

db.airlines_reviews.aggregate([
  {
    $addFields: {
      ComplaintType: {
        $switch: {
          branches: [
            { case: { $regexMatch: { input: "$Reviews", regex: /delay/i } }, then: "Delay" },
            { case: { $regexMatch: { input: "$Reviews", regex: /lost baggage|luggage/i } }, then: "Lost Baggage" },
            { case: { $regexMatch: { input: "$Reviews", regex: /rude|unfriendly/i } }, then: "Rude Staff" },
            { case: { $regexMatch: { input: "$Reviews", regex: /legroom|cramped/i } }, then: "Lack of Comfort" },
            { case: { $regexMatch: { input: "$Reviews", regex: /food|meal/i } }, then: "Poor Food Quality" }
          ],
          default: "Other"
        }
      }
    }
  },
  {
    $group: {
      _id: { Airline: "$Airline", TypeofTraveller: "$TypeofTraveller", ComplaintType: "$ComplaintType" },
      Complaint_Frequency: { $sum: 1 }
    }
  }
])



db.airlines_reviews.aggregate([
  {
    $addFields: {
      ComplaintType: {
        $switch: {
          branches: [
            { case: { $regexMatch: { input: "$Reviews", regex: /delay/i } }, then: "Delay" },
            { case: { $regexMatch: { input: "$Reviews", regex: /lost baggage|luggage/i } }, then: "Lost Baggage" },
            { case: { $regexMatch: { input: "$Reviews", regex: /rude|unfriendly/i } }, then: "Rude Staff" },
            { case: { $regexMatch: { input: "$Reviews", regex: /legroom|cramped/i } }, then: "Lack of Comfort" },
            { case: { $regexMatch: { input: "$Reviews", regex: /food|meal/i } }, then: "Poor Food Quality" }
          ],
          default: "Other"
        }
      }
    }
  },
  {
    $group: {
      _id: { Airline: "$Airline", TypeofTraveller: "$TypeofTraveller", ComplaintType: "$ComplaintType" },
      Complaint_Frequency: { $sum: 1 }
    }
  },
  {
    $sort: { "_id.Airline": 1, "_id.TypeofTraveller": 1, "Complaint_Frequency": -1 }  // Sort by Airline, TypeofTraveller and frequency in descending order
  },
  {
    $group: {
      _id: { Airline: "$_id.Airline", TypeofTraveller: "$_id.TypeofTraveller" },
      Complaints: {
        $push: {
          ComplaintType: "$_id.ComplaintType",
          Complaint_Frequency: "$Complaint_Frequency"
        }
      }
    }
  }
])


db.airlines_reviews.aggregate([
  {
    $addFields: {
      ComplaintType: {
        $switch: {
          branches: [
            { case: { $regexMatch: { input: "$Reviews", regex: /delay/i } }, then: "Delay" },
            { case: { $regexMatch: { input: "$Reviews", regex: /lost baggage|luggage/i } }, then: "Lost Baggage" },
            { case: { $regexMatch: { input: "$Reviews", regex: /rude|unfriendly/i } }, then: "Rude Staff" },
            { case: { $regexMatch: { input: "$Reviews", regex: /legroom|cramped/i } }, then: "Lack of Comfort" },
            { case: { $regexMatch: { input: "$Reviews", regex: /food|meal/i } }, then: "Poor Food Quality" }
          ],
          default: "Other"
        }
      }
    }
  },
  {
    $group: {
      _id: { Airline: "$Airline", TypeofTraveller: "$TypeofTraveller", ComplaintType: "$ComplaintType" },
      Complaint_Frequency: { $sum: 1 }
    }
  },
  {
    $sort: { "_id.Airline": 1, "_id.TypeofTraveller": 1, "Complaint_Frequency": -1 }  // Sort by Airline, TypeofTraveller and frequency in descending order
  },
  {
    $group: {
      _id: { Airline: "$_id.Airline", TypeofTraveller: "$_id.TypeofTraveller" },
      Complaints: {
        $push: {
          ComplaintType: "$_id.ComplaintType",
          Complaint_Frequency: "$Complaint_Frequency"
        }
      }
    }
  },
  {
    $project: {
      Airline: "$_id.Airline",
      TypeofTraveller: "$_id.TypeofTraveller",
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
  }
])
