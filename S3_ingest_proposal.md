This is a suggestion/proposal for how we structure our S3 ingest bucket and how we manage the data that flows through that bucket.  Other options are definitely available!


Create 3 'folder' areas within the ingestion bucket:

1. baseline.  This holds the very first ingest of all of the data, and should be instantly copied to latest to provide the first set of data for processing.
2. staging.  This holds data that has changed within the last 20 minutes, in a log file
3. latest.  This holds all of the data in a state that is good for the processing lambda to fetch. 

After the initial full ingest of all data into baseline, the ingest lamda should follow this process for each table, I think we said on a 20 minute schedule:

`Select * from table where last_updated in last 20 mins`

`If table_id NOT IN latest version of the data, then add row to latest for that table`

This will add any new rows (all columns) to the latest version of the data, no history required as the 'created at' column will already hold this information.  Table-id in this scenario is the primary key id column for each table.


`If table_id IN latest version of the data, then write the change to the json.log`
This will create a record of what has changed in any tables where a change is detected.

Log file in the following format, in the 'staging folder' of S3

|change_id |table_name |column_name |record_id |last_value |new_value | date/time_of_change |
|----------|-----------|------------|-----------|----------|----------| --------------------|
|001|counterparty|commercial_contact |861 |john smith|fred bloggs| 2021-05-19-random-time|

This may be simpler with a separate change log for each table.  Then if we make the log files available to the processing lambda it has a copy of the latest snapshot of the data, plus the change log for each table.  In that case we wouldn't need the table_name column.

The data processing lambda will need to check in the 'latest' folder for the most recent snapshot of all data, plus in the log file for any changes to specific record_id rows.

Where a change is detected:

if the changed item belongs to a fact table then it will add a new entry to the fact_sales_order table (create a new sales_record_id) but will still link to the same sales_order_id, which will maintain referential integrity/history for all changes on the same sales_order_id record.  Eg

say sales_order_id 17 has a change to the agreed delivery location.  It was originally agreed to be delivered to delivery_location_id 3 but now it will be delivered to delivery_location_id 5.
In the fact table the first agreed delivery to location 3 will have a unique sales_record_id number, and that will reference sales_order_id 17.
When the change is made, a new entry is made into the fact table with a new sales_record_id number, but this new record will still reference sales_order_id 17.

