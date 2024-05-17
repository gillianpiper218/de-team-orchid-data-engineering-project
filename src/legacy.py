# This can be deleted, legacy plan
# def initial_data_for_latest(table_names=get_table_names(), bucket_name=S3_BUCKET_NAME):
#     for table in table_names:
#         s3.copy_object(
#             Bucket=bucket_name,
#             CopySource=f"{bucket_name}/baseline/{table[0]}.json",
#             Key=f"latest/{table[0]}.json",
#         )


# def get_s3_object_data(key):
#     # try:
#     response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
#     data = json.loads(response["Body"].read().decode("utf-8"))
#     return data
# # except:


# def update_latest_with_new_record():
#     staging_response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="staging/")
#     list_of_staging_files = []
#     for s_item in staging_response["Contents"]:
#         if s_item["Size"] > 2:
#             list_of_staging_files.append(s_item["Key"][8:])
#         # pprint.pp(list_of_staging_files)

#     if list_of_staging_files == []:
#         logger.info("No new files")
#         # print("No new files")
#         # pprint.pp(list_of_staging_files)
#     else:
#         latest_response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="latest/")
#         list_of_latest_files = []
#         for l_item in latest_response["Contents"]:
#             if l_item["Key"][7:] in list_of_staging_files:
#                 list_of_latest_files.append(l_item["Key"][7:])

#         for item in list_of_staging_files:
#             staging_data = get_s3_object_data(f"staging/{item}")
#             latest_data = get_s3_object_data(f"latest/{item}")

#             col_id_name = re.sub(r"\.json", "_id", item)
#             biggest_id_dict = max(latest_data, key=lambda x: x[col_id_name])

#             # pprint.pp(biggest_id_dict)
#             # print(">>>>>>>>>>>>>>>>>>>>>>>>>>")
#             for el in staging_data:
#                 pprint.pp(el)
#                 if el[col_id_name] > biggest_id_dict[col_id_name]:
#                     latest_data.append(el)
#                     logger.info("new record added to latest")
#             data = json.dumps(latest_data)
#             file_path = f"latest/{item}"
#             s3.put_object(Body=data, Bucket=S3_BUCKET_NAME, Key=file_path)

# class TestInitialDataForLatest:

#     @pytest.mark.it("unit test: baseline contents copied into latest")
#     def test_copies_from_baseline(self, s3):
#         table_names = get_table_names()
#         test_bucket_name = "test_bucket"

#         s3.create_bucket(
#             Bucket=test_bucket_name,
#             CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
#         )
#         test_body = "hello"
#         for table in table_names:
#             s3.put_object(
#                 Bucket=test_bucket_name, Key=f"baseline/{table[0]}.json", Body=test_body
#             )

#         initial_data_for_latest(
#             bucket_name=test_bucket_name, table_names=get_table_names()
#         )

#         for table in table_names:
#             response = s3.get_object(
#                 Bucket=test_bucket_name, Key=f"latest/{table[0]}.json"
#             )
#             assert response["Body"].read().decode("utf-8") == "hello"

#     @pytest.mark.it("unit test: check the keys in latest match to table names")
#     def test_correct_keys_in_latest(self, s3):
#         table_names = get_table_names()
#         test_bucket_name = "test_bucket"

#         s3.create_bucket(
#             Bucket=test_bucket_name,
#             CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
#         )
#         test_body = "hello"
#         for table in table_names:
#             s3.put_object(
#                 Bucket=test_bucket_name, Key=f"baseline/{table[0]}.json", Body=test_body
#             )

#         initial_data_for_latest(
#             bucket_name=test_bucket_name, table_names=get_table_names()
#         )

#         response = s3.list_objects_v2(Bucket=test_bucket_name, Prefix="latest")

#         for i in range(len(response["Contents"])):
#             assert response["Contents"][i]["Key"] == f"latest/{table_names[i][0]}.json"
