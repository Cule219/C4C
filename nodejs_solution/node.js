// import csv-parser module to help us parse csv and file system so we can load the csv file and finaly csv-writer so we can output our results
const csvp = require("csv-parser");
const fs = require("fs");
const csvw = require("csv-writer").createObjectCsvWriter;

// global store variable, here we use memoization for storing values
let store = {};
// header data for csv result file
let header = [
  {
    id: "category",
    title: "Category",
  },
  {
    id: "count",
    title: "Count",
  },
  {
    id: "earliest_date",
    title: "Earliest Date",
  },
  {
    id: "latest_date",
    title: "Latest Date",
  },
];
// create file stream and pipe it into csv module that will fire an event each time we encounter new line
fs.createReadStream("../C4C-dev-challenge-2018.csv")
  .pipe(csvp())
  .on("data", (row) => {
    // for each row, we check if it's already in store, if it is we push the row if not we create new category in our store with our row
    store[row.violation_category]
      ? store[row.violation_category].push(row)
      : (store[row.violation_category] = [row]);
  })
  // Once the file has been read we start writing new csv file to store the results
  // Sort the cases based on date occurred and count all the categories
  .on("end", () => {
    const csvWriter = csvw({
      path: "result.csv",
      header,
    });
    const data = [];
    Object.keys(store).forEach((key) => {
      store[key].sort(
        (a, b) => new Date(a.violation_date) - new Date(b.violation_date)
      );
      // Prepare data for the csv result file
      data.push({
        category: key,
        count: store[key].length,
        earliest_date: store[key][0].violation_date,
        latest_date: store[key][store[key].length - 1].violation_date,
      });

      // Uncomment the lines bellow to see the results in the console
      /** console.log(
        `${key} # of cases: ${store[key].length}, earliest violation_date: ${
          store[key][0].violation_date
        }, latest violation_date: ${
          store[key][store[key].length - 1].violation_date
        }`
      ); */
    });
    // Write the data into result csv file
    csvWriter
      .writeRecords(data)
      .then(() => console.log("The CSV file was written successfully"));
  });
