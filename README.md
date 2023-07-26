# Restaurant Uptime and Downtime Monitoring API

## Problem Statement

Loop monitors several restaurants in the US and needs to monitor if the store is online or not. All restaurants are supposed to be online during their business hours. Due to some unknown reasons, a store might go inactive for a few hours. Restaurant owners want to get a report of how often this happened in the past.

We want to build backend APIs that will help restaurant owners achieve this goal.

## Data Sources

We will have 3 sources of data:

1. Store Status: We poll every store roughly every hour and have data about whether the store was active or not in a CSV. The CSV has 3 columns (`store_id, timestamp_utc, status`) where status is active or inactive. All timestamps are in **UTC**.

   - Data source: [Store Status CSV](https://drive.google.com/file/d/1UIx1hVJ7qt_6oQoGZgb8B3P2vd1FD025/view?usp=sharing)

2. Store Business Hours: We have the business hours of all the stores - the schema of this data is `store_id, dayOfWeek(0=Monday, 6=Sunday), start_time_local, end_time_local`.

   - These times are in the **local time zone**.
   - If data is missing for a store, assume it is open 24*7.

   - Data source: [Store Business Hours CSV](https://drive.google.com/file/d/1va1X3ydSh-0Rt1hsy2QSnHRA4w57PcXg/view?usp=sharing)

3. Store Timezone: Timezone for the stores - schema is `store_id, timezone_str`.

   - If data is missing for a store, assume it is America/Chicago.
   - This is used so that data sources 1 and 2 can be compared against each other.

   - Data source: [Store Timezone CSV](https://drive.google.com/file/d/101P9quxHoMZMZCVWQ5o-shonk2lgK1-o/view?usp=sharing)

## System Requirement

- Do not assume that this data is static and precompute the answers as this data will keep getting updated every hour.
- You need to store these CSVs in a relevant database and make API calls to get the data.

## Data Output Requirement

We want to output a report to the user that has the following schema:

`store_id, uptime_last_hour(in minutes), uptime_last_day(in hours), update_last_week(in hours), downtime_last_hour(in minutes), downtime_last_day(in hours), downtime_last_week(in hours)`

1. Uptime and downtime should only include observations within business hours.
2. You need to extrapolate uptime and downtime based on the periodic polls we have ingested, to the entire time interval.
   - For example, if the business hours for a store are 9 AM to 12 PM on Monday, and we only have 2 observations for this store on a particular date (Monday) at 10:14 AM and 11:15 AM, we need to fill the entire business hours interval with uptime and downtime from these 2 observations based on some sane interpolation logic.

Note: The data we have given is a static data set, so you can hard code the current timestamp to be the max timestamp among all the observations in the first CSV.

## API Requirement

You need two APIs:

1. `/trigger_report` endpoint that will trigger report generation from the data provided (stored in DB).
   - No input is required.
   - Output - report_id (random string).
   - The `report_id` will be used for polling the status of report completion.

2. `/get_report` endpoint that will return the status of the report or the CSV.
   - Input - `report_id`.
   - Output:
     - If report generation is not complete, return “Running” as the output.
     - If report generation is complete, return “Complete” along with the CSV file with the schema described above.

## Considerations/Evaluation Criteria

1. The code should be well-structured, handling corner cases, with a good type system.
2. The functionality should be correct for trigger + poll architecture, database reads, and CSV output.
3. The logic for computing the hour's overlap and uptime/downtime should be well-documented and easy to read/understand.
4. The code should be optimized and run within a reasonable amount of time.

## Proposed Solution

To address the problem statement, we will build a RESTful API using the FastAPI framework. The API will be responsible for fetching data from the provided CSVs, storing them in a database, and generating reports on request.

The API will provide the following endpoints:

1. `/trigger_report`: This endpoint will trigger the report generation process. It will start the background task to compute the required statistics for each store and save the results in a CSV file. The API will return a unique `report_id` that can be used to query the status of the report later.

2. `/get_report`: This endpoint will allow users to check the status of the report generation. If the report is still being computed, it will return "Running". If the report is complete, it will provide the link to download the CSV file containing the report.

To compute the report, we will use the provided data sources: `store_status`, `store_business_hours`, and `store_timezone`. The data will be fetched from the CSV files and stored in a database using SQLAlchemy. The report computation will involve processing the data for each store, calculating uptime and downtime within business hours, and then extrapolating the results to the entire time interval. The final report will be saved in a CSV file and provided to the user for download.

The solution will be implemented in Python using efficient data processing techniques to optimize the computation time. The code will be well-organized, documented, and tested to ensure correctness and reliability. The API will be designed to handle concurrent requests efficiently.

Overall, the proposed solution will provide an efficient and reliable API for monitoring restaurant uptime and downtime, enabling restaurant owners to get timely and accurate reports on their store activities.
