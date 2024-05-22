This is a suggestion/proposal for how we structure our S3 ingest bucket and how we manage the data that flows through that bucket.  Other options are definitely available!


Create 5 'folder' areas within the ingestion bucket:

1. baseline.  This holds the very first ingest of all of the data, and should be instantly copied to latest to provide the first set of data for processing.
2. staging.  This holds the initial fetch of data that has changed within the last 20 minutes.  Once these files are processed they can be deleted. 
3. archive.  This holds processed staging files.
4. latest.  This holds all of the most up to date data in a state that is good for the processing lambda to fetch. 
5. history.  This will hold historical data in audit-type tables.

## Attempt to explain the change process - 2nd go!!

Now that we have functions that put data into 3 of the 5 areas mentioned above, we can try and create a process to create the change logs and to update 'latest'.  The details below use the staff table as a worked example, but we need to follow this process for all of the tables.

Every 20 mins, the staging/staff.json file is refreshed.  The lambda should check the files on a 20 minute schedule.

If staging/staff.json IS EMPTY, then:
    Log the fact that the file has been checked and found to be empty
    Delete the file - no point archiving empty files.

If staging/staff.json IS NOT EMPTY then     compare staging/staff.json/staff_id values to the values in latest/staff.json/staff_id:
    
    For each id_value in the staging file (ie loop through the staging id values):

        if the id value DOES NOT exist in latest/staff.json, then APPEND the whole row for that id to latest/staff.json
        Log this as a successful append to latest/staff.json
        Handle the error if unable to append the row

        if the id value DOES exist in latest/staff.json/staff_id, then:
            . EXTRACT (pop?) the 'old' (existing) row out of latest/staff.json and APPEND it to history/staff.json
            Log this as a successful removal from latest/staff.json and successful APPEND to history/staff.json
            Handle any errors removing or appending.

            . APPEND the new data (whole row) for the staging id value to latest/staff.json - latest now has the most recent version of the data for that staff_id
            Log this as a successful append to latest/staff.json
            Handle any errors appending.

        Continue the loop dealing with each staff_id listed in the latest file.  When you've looped through all of the id values in latest/staff.json and done something with the data for each of the values:
            Move the file out of staging and into archive with a new filename that adds a date/timestamp.
            Log this as a successful archive
            Handle any errors where archive is unsuccessful
        
        As long as 20 mins is long enough to work through every table, then we should end up with:
        . an empty 'staging' area
        . an archive area containing date/timestamped files for any tables that had changes in last 20 mins
        . a full copy of latest data for all of the tables in latest
        . logs confirming success of each action
        . errors if something went wrong.  Nb if something does go wrong, we might want to pause the ingest schedule to give us time to resolve it.  Need to think that through.

## Thinking ahead slightly

Our 'transform' lambda will need to collect up both the 'latest' files and the 'history' files for each table, and stitch them together into fact and dim tables.  This will work because the fact and dim tables will have a new unique id for every action, and the same staff_id (for example) which is unique in the operational database, can have multiple actions against it so doesn't need to be unique in the data warehouse for reporting.


            
        




## Old explanation below - this is archived.
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






