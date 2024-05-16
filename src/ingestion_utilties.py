import boto3
import json


def ingest_to_baseline():
    pass


# 1 create a function for the first ingestion to puts all the data in the baseline
# 2 creat a function to copy basline into latest
# 3 write another function to pick up tables that have changed and check aginst latest
# 4 if there is a new record add to latest
# 5  for each table if there is  a chnage in the record
# 5.1 take copy of latest for that record
# 5.2 write the copy to the change log
# 5.3 add the updated record to the latest
# format of the data files of the bucket
