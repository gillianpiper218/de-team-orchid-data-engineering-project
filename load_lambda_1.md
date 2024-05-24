# Loading Lambda Handler Function Thoughts/ Proposal

This Lambda is to be triggered by a scheduler using CloudWatch.

Processed s3 bucket has one file/path for the fact table and dim tables in parquet format, which are continually being appended to with updates.

## Db connection function

- One function that uses the dw credentials to get_db_connection of the warehouse we need to load data into.

- Handle error of failed database connection, logging also.

## Get processed dim tables from S3 bucket then load into warehouse

- One function that loops over 'table_name' in defined list of mvp dimension tables and reads their parquet files from the processed s3 bucket. Just so we don't have multiple functions for dim tables doing the same thing.

- example key = f’dimension/{table_name}.parquet’, to input into s3.get_object(…) to read the parquet file/obtain the dim table.

- Convert this dim table to a DataFrame 'df' to then load into the data warehouse.

    - Use a separate function that handles loading any DataFrame into the data warehouse - for example a function named load_to_warehouse(df, table_name), taking the DataFrame and table name as arguments.

    - load_to_warehouse(df, table_name) will be invoked within this function.

## Get processed fact table from S3 bucket then load into warehouse

- One function, same method as with the dim tables.

## Load_to_warehouse general use function

- Connect to the database and set up a cursor.

- Load DataFrame into the data warehouse using something similar to:
`df.to_sql(‘my_cool_table’, con=cnx, index=**False**)` or `df.to_sql(name='users', con=connection, if_exists='append')` Obtained from: https://blog.panoply.io/how-to-load-pandas-dataframes-into-sql and https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html

- Or execute an INSERT INTO 'table_name' query, but would need to search how to create placeholders for the VALUES (column names) for each table, and how to go about obtaining this from the column names of the dim table / dim table DataFrame. Not sure at this stage which is most appropriate, the first option appears simpler.

- conn.commit() needed to save changes by SQL queries: https://www.geeksforgeeks.org/python-postgresql-transaction-management-using-commit-and-rollback/

- logger.info successful loads, logger.error any failed loads with exceptions.

- End of function: close cursor, close connection. 

## Actual lambda handler function
Along the lines of:
```python 
def lambda_handler(event, context):  
    load_dim_tables()  
    load_fact_table()
```
- Invokes ‘Get processed dim tables from S3 bucket then load into warehouse’ function and ‘Get processed fact table from S3 bucket then load into warehouse’ function.

- Catch and log potential errors.