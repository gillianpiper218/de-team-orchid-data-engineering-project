# Loading Lambda Handler Function Thoughts/ Proposal 2

This Lambda is to be triggered by a scheduler using CloudWatch.

Processed s3 bucket has files/ file paths/ keys for the fact table and dim tables in parquet format, which are timestamped.

## Db connection function

```python
def get_db_connection():
```

- One function that uses the dw credentials to get_db_connection of the warehouse we need to load data into.

- Handle error of failed database connection, logging also.

## Retreive latest parquet file key from processed s3 bucket function
```python
def get_latest_parquet_file(bucket, prefix):
```

- Multiple timestamped parquet files in fact/sales_order and dimension/{table_name}.

- Response = s3.list_object_v2(bucket, prefix).

- Logic to sort timestamped responses and get lastest file key.

## Read parquet files from processed s3 function
```python
def read_parquet_from_s3(bucket, key)
```

- Uses `get_latest_parquet_file` function to get the latest key, to input into s3.get_object(...) call.

- Response = s3.get_object(bucket, key).

- Get response object body table data.

## Get processed dim tables from S3 bucket then load into warehouse function
```python
def load_dim_tables(bucket)
```

- One function that loops over 'table_name' in defined list of mvp dimension tables and reads their parquet files from the processed s3 bucket.

    - Should have three functions within this, it uses the `get_latest_parquet_file(bucket, prefix)` function, and `read_parquet_from_s3(bucket, key)` function to get a files key and its df to be passed into the `Load_to_warehouse(table_data, table_name)` function also being invoked within this function:
        - Broken down further:

            - `get_latest_parquet_file(bucket, prefix)` outputs a **key**

            - **key** for example = f’dimension/{table_name}.parquet’, to input into `read_parquet_from_s3(bucket, key)` to read the parquet file/obtain the dim table.

            - `read_parquet_from_s3(bucket, key)` outputs the dim table parquet file ready to be loaded into the data warehouse it is inputted into `Load_to_warehouse(table_data, table_name)` to load into the dw.

            - This is within a loop, so it should do this for each dim table. 


- What `load_to_warehouse(table_data, table_name)`, function should do:

    - A separate function that handles loading any table_data into the data warehouse, taking the table_data and 'table_name' as arguments.

- Additonal note that may be needed: Check if the table file has been loaded with tagging, if yes, `continue` / skip that file, before invoking loading to dw function, then mark file as loaded.

## Get processed fact table from S3 bucket then load into warehouse function
```python
def load_fact_table(bucket)
```

- One function, same method as with the dim tables, but it won't need looping as we only have one fact table. 

## Load_to_warehouse general use function
```python
def load_to_warehouse(table_data, table_name)
```

- Connect to the database with `get_db_connection` function and set up a cursor.

- Execute an INSERT INTO 'table_name' query, but would need to search how to create placeholders for the VALUES (column names) for each table, and how to go about obtaining this from the column names of the dim table.

- conn.commit() needed to save changes by SQL queries: https://www.geeksforgeeks.org/python-postgresql-transaction-management-using-commit-and-rollback/

- logger.info successful loads, logger.error any failed loads with exceptions.

- End of function: close cursor, close connection. 

## Actual lambda handler function
Along the lines of:
```python 
def lambda_handler(event, context):  

    load_dim_tables(bucket)  
    load_fact_table(bucket)
```
- Invokes ‘Get processed dim tables from S3 bucket then load into warehouse’ function and ‘Get processed fact table from S3 bucket then load into warehouse’ function.

- Catch and log potential errors.