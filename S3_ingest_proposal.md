This is a suggestion/proposal for how we structure our S3 ingest bucket and how we manage the data that flows through that bucket.  Other options are definitely available!


Create 3 'folder' areas within the ingestion bucket:

1. baseline.  This holds the very first ingest of all of the data, and should be instantly copied to latest to provide the first set of data for processing.
2. changes_staging.  This holds data that has changed within the last 20 minutes, in a log file.  This will retain a record of historical values
3. latest.  This holds all of the data in a state that is good for the processing lambda to fetch. 

After the initial full ingest of all data into baseline, the ingest lamda should follow this process for each table, I think we said on a 20 minute schedule:

`Select * from table where last_updated in last 20 mins`

`If tablename_id (=primary key for that table) NOT IN latest version of the data, then add row to 'latest' for that table`

This will add any new rows (all columns) to the latest version of the data, no history required as the 'created at' column will already hold this information.  Table-id in this scenario is the primary key id column for each table.


`If tablename_id IN latest version of the data, then write the current/old value to the changelog file for that table in the same json format.
This will create a record of what the previous version of the record was changed in any tables where a change is detected.  The tablename_id will remain with that record in the changelog, so that the fact table can pick this up later and create a record of it.

Each table will have its own changelog file in this changes_staging area - although some tables may not need it, ie may never change.

Once we have a copy of the old value of that record in the change log, then we can add the new version of the record into latest.  I think the process might need to be to remove that record id from latest, add it into change log, add the changed record into latest.


The data processing lambda will need to check in the 'latest' folder for the most recent snapshot of all data, plus in the changelog file for any changes to specific record_id rows.  Each of these will require a record in the fact table.

Where a change is detected:

if the changed item belongs to a fact table then it will add a new entry to the fact_sales_order table (create a new sales_record_id) but will still link to the same sales_order_id, which will maintain referential integrity/history for all changes on the same sales_order_id record.  Eg

say sales_order_id 17 has a change to the agreed delivery location.  It was originally agreed to be delivered to delivery_location_id 3 but now it will be delivered to delivery_location_id 5.
In the fact table the first agreed delivery to location 3 will have a unique sales_record_id number, and that will reference sales_order_id 17.
When the change is made, a new entry is made into the fact table with a new sales_record_id number, but this new record will still reference sales_order_id 17.

