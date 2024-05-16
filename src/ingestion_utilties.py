
# 1 create a function for the first ingestion to puts all the data in the baseline
# 2 creat a function to copy basline into latest
# 3 write another function to pick up tables that have changed and check aginst latest
# 4 if there is a new record add to latest
# 5  for each table if there is  a chnage in the record
# 5.1 take copy of latest for that record
# 5.2 write the copy to the change log
# 5.3 add the updated record to the latest
# format of the data files of the bucket


def get_s3_object_data(key):
    # try:
    response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    data = json.loads(response["Body"].read().decode("utf-8"))
    return data
    # except s3.exceptions.NoSuchKey:
    

def update_latest_with_new_record():
    staging_response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="staging/")
    list_of_staging_files = []
    for s_item in staging_response["Contents"]:
        if s_item["Size"] > 2:
            list_of_staging_files.append(s_item["Key"][8:])
        #pprint.pp(list_of_staging_files)

    if list_of_staging_files == []:
        logger.info("No new files")
        #print("No new files")
        #pprint.pp(list_of_staging_files)
    else:
        latest_response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="latest/")
        list_of_latest_files = []
        for l_item in latest_response["Contents"]:
            if l_item["Key"][7:] in list_of_staging_files:
                list_of_latest_files.append(l_item["Key"][7:])

        for item in list_of_staging_files:
            staging_data = get_s3_object_data(f'staging/{item}')
            latest_data = get_s3_object_data(f'latest/{item}')

            col_id_name = re.sub(r'\.json', '_id', item)
            biggest_id_dict = max(latest_data, key=lambda x: x[col_id_name])


            #pprint.pp(biggest_id_dict)
            #print(">>>>>>>>>>>>>>>>>>>>>>>>>>")
            for el in staging_data:
                pprint.pp(el)
                if el[col_id_name] > biggest_id_dict[col_id_name]:
                    latest_data.append(el)
                    logger.info("new record added to latest")
            data = json.dumps(latest_data)
            file_path = f"latest/{item}"
            s3.put_object(Body=data, Bucket=S3_BUCKET_NAME, Key=file_path)

