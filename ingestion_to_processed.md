# Ingestion to Processed Plan
Below is the "Transform" plan. Data from the "Ingestion" S3 bucket will need to be used to populate the "Processed" S3 bucket.
Ultimately the lambda will need to:
- take the json file
- add/remove necessary keys
- convert to parquet

## Structure of Ingestion S3
- baseline
    - table_name-timestampe.json
- updated
    - table_name-timestamp.json

## Proposed Structure of Processed S3
- fact
    - sales_order.parquet
- dimension
    - date.parquet
    - design.parquet
    - counterparty.parquet
    - currency.parquet
    - location.parquet
    - staff.parquet

The contents of each file will need to be inline with the columns in the star schema below.
## Sales Star Schema Structure
![alt text](erd_diagrams/Sales%20Schema.jpg)

## Details for each Star Schema Table
### fact_sales_order
The fact_sales_order table will be populated by the dat from the fact_sales_order table.
There are some additional columns to be created in the fact_sales_order:
- Sales_order table contains columns:
    - created_at
    - last_updated

- These need to be split out into:
    - created_date
    - created_time
    - last_updated_date
    - last_updated_time

### dim_date
The dim_date table will be populated by the data from the fact_sales_order table.
Each date will be given a unique id (date_id) and then split out into the following columns:
- year
- month
- day
- day_of_week (int type where Monday = 1 and Sunday = 7)
- day_name
- month_name
- quarter

### dim_design
The dim_design table will be populated by the data from the design table with the following adjustments:
- created_at not required for dim_design
- last_updated not required for dim_design

### dim_counterparty
The dim_counterparty table will be populated by the data from the counterparty table with the following adjustments:
- commercial_contact not required for dim_counterparty
- delivery_contact not required for dim_counterparty
- created_at not required for dim_counterparty
- last_updated not required for dim_counterparty

### dim_currency
The dim_currency table will be populated by the data from the currency table with the following adjustments:
- **currency_name column will need to be created**
- created_at not required for dim_currency
- last_updated not required for dim_currency

### dim_location
The dim_location table will be populated by the data from the address table.
The following adjustments will need to be made:
- **address_id (from address table) will become location_id**
- created_at not required for dim_location
- last_updated not required for dim_location

### dim_staff
The dim_staff table will be populated by the data from the staff table with the following adjustments:
- created_at not required for dim_staff
- last_updated not required for dim_staff

### Notes
You should create new data files containing additions or amendments.

A Python application that remodels at least some of the data into a predefined schema suitable for a data warehouse and stores the data in Parquet format in the "processed" S3 bucket.

for your minimum viable product, you need only populate the following:

tablename
fact_sales_order
dim_staff
dim_location
dim_design
dim_date
dim_currency
dim_counterparty

Your warehouse should contain a full history of all updates to facts. For example, if a sales order is created in totesys and then later updated (perhaps the units_sold field is changed), you should have two records in the fact_sales_order table. It should be possible to see both the original and changed number of units_sold. It should be possible to query either the current state of the sale, or get a full history of how it has evolved (including deletion if applicable).

It is not necessary to do this for dimensions (which should not change very much anyway). The warehouse should just have the latest version of the dimension values. However, you might want to keep a full record of changes to dimensions in the S3 buckets.

A Python application to transform data landing in the "ingestion" S3 bucket and place the results in the "processed" S3 bucket. The data should be transformed to conform to the warehouse schema (see above). The job should be triggered by either an S3 event triggered when data lands in the ingestion bucket, or on a schedule. Again, status and errors should be logged to Cloudwatch, and an alert triggered if a serious error occurs.

converting json to parquet: https://medium.com/@turkelturk/tjson-to-parquet-in-python-a3d2a6abc5c6
appending parquet file: https://mathdatasimplified.com/efficient-data-appending-in-parquet-files-delta-lake-vs-pandas/#:~:text=Appending%20data%20to%20an%20existing,to%20recreate%20the%20entire%20table.