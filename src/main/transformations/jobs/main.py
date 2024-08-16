import datetime
import os
import shutil
import sys

from pyspark.sql.functions import concat_ws, lit, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_mart_sql_transformation_write import sales_mart_sql_transformation_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import *
from src.main.utility.s3_client_object import *
from src.main.utility.my_sql_session import *
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter

##########Getting S3 Client###############
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()
print(response)
logger.info("List of Buckets: %s", response["Buckets"])

#Check if local directory has already a file
#If file is present in local then check if the same file is present in the STG table
#If status is A then the process stopped due to some issue. So try rerun.
#Else Give an error message saying file failed to load to S3 and dont process the next file. (Exit 1)

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith("csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_file = []
if csv_files:
    for file in csv_files:
        total_csv_file.append(file)

    statement_inactive = f"""
    select distinct file_name from
    {config.database_name}.{config.product_staging_table}
    where file_name in ({str(total_csv_file)[1:-1]}) and status='I'
    """
    statement_active = f"""
    select distinct file_name from
    {config.database_name}.{config.product_staging_table}
    where file_name in ({str(total_csv_file)[1:-1]}) and status='A'
    """
    logger.info(f"Dinamically query created: {statement_inactive}")
    cursor.execute(statement_inactive)
    data = cursor.fetchall()
    if data:
        logger.info("Your last process failed to load. Please check.")
    else:
        logger.info("File not found in Inactive State")
        cursor.execute(statement_active)
        data_active = cursor.fetchall()
        if data_active:
            logger.info(f"File is in Active State. Dynamically changed the query {statement_active}")
            logger.info("Try to rerun it")
        else:
            logger.info("File not found in Active State")
            logger.info("No records match. File not found")
else:
    logger.info("Execution Successfull !!")

try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    #print(folder_path)
    s3_absolute_file_path = s3_reader.list_files(s3_client,config.bucket_name,config.s3_source_directory)
    logger.info(f"Absolute path on S3 Bucket for csv file {s3_absolute_file_path}")
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No data available to process.")
except Exception as e:
    logger.error(f"Exited with error:- {e}")
    raise e

bucket_name = config.bucket_name
local_dir = config.local_directory

prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logger.info(f"File path available in S3 bucket: {bucket_name} with folder name is {file_paths}")
try:
    downloader = S3FileDownloader(s3_client,bucket_name,local_dir)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error(f"Error in downloading file: {e}")
    sys.exit()

#Get a list of all the files in the local directory
all_files = os.listdir(local_dir)
logger.info(f"List of files present in my local directory after downloading: {all_files}")

#Seperating the csv files from the other files and creating their absolute paths
if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_dir,files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_dir,files)))

    if not csv_files:
        logger.error("No csv files available to process.")
        raise Exception("No csv files available to process")

else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")

logger.info("================================Listing the CSV Files================================")
logger.info(f"List of all the CSV Files: {csv_files}")

logger.info("================================Creating Spark Session================================")
spark=spark_session()
logger.info("================================Spark Session Created================================")

logger.info("================================SCHEMA VALIDATION STARTED===================================")

#Check for the Required columns in a the csv files schema
#If we did not get the required columns then the csv file will be pushed to the error directory
#if we get the required columns but also get some extra columns then we keep it as a list in an extra column.
#Else if every column matches or if we have handeled the extra columns then we union all the data into 1 dataframe.

logger.info("===============================Checking Schema for Data load in S3============================")

corrected_files = []
for data in csv_files:
    data_schema = spark.read.format("csv")\
        .option("header",True)\
        .load(data).columns
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory Columns are: {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns)-set(data_schema)
    logger.info(f"Missing Columns are: {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing columns found for: {data}")
        corrected_files.append(data)

logger.info(f"List of Corrected Files: {corrected_files}")
logger.info(f"List of Error Files: {error_files}")
logger.info("===============================Moving Error Data to Error Directory============================")
#Moving the error files to error directory in local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(config.error_folder_path_local,file_name)

            shutil.move(file_path,destination_path)
            logger.info(f"File {file_name} moved from S3 local file path to Error local file path: {destination_path}")
#Moving the error files to the error directory in S3
            s3_source = config.s3_source_directory
            s3_error = config.s3_error_directory

            message = move_s3_to_s3(s3_client,bucket_name,s3_source,s3_error)
            logger.info(f"{message}")
        else:
            logger.info(f"{file_path} does not exists")
else:
    logger.info("===============================There are no error files available at our dataset============================")

#Before starting any process we need to register the files available in stg table with their state.
logger.info("===============================Updating the product_stg_table for starting the process===============================")
insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if corrected_files:
    for file in corrected_files:
        filename = os.path.basename(file)
        statements = f"""
        INSERT INTO {db_name}.{config.product_staging_table} (file_name, file_location, created_date, status)
        VALUES ('{filename}','{file}','{formatted_date}','A')
        """
        insert_statements.append(statements)

    logger.info("===============================Insert statements created for STG table============================")
    logger.info(f"{insert_statements}")
    logger.info("===============================Connecting with MYSQL Server============================")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("===============================MYSQL Server connected Successfully============================")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("==============================There is no data to process===============================")
    raise Exception("===========================No data availavle in corrected folder to process====================")

logger.info("==============================Staging table Updated Successfully===============================")

logger.info("=================================Fixing Extra columns coming from the source==============================")
schema = StructType([
    StructField("customer_id",IntegerType(),True),
    StructField("store_id",IntegerType(),True),
    StructField("product_name",StringType(),True),
    StructField("sales_date",DateType(),True),
    StructField("sales_person_id",IntegerType(),True),
    StructField("price",FloatType(),True),
    StructField("quantity",IntegerType(),True),
    StructField("total_cost",FloatType(),True),
    StructField("additional_columns",StringType(),True)
])

logger.info("==================================Creating empty Data Frame===================================")
final_df_to_process = spark.createDataFrame([],schema=schema)
final_df_to_process.show()
logger.info("==================================Empty Data Frame Created===================================")
for data in corrected_files:
    data_df = spark.read.format("csv")\
            .option("inferSchema",True)\
            .option("header",True)\
            .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema)-set(config.mandatory_columns))
    logger.info(f"Extra columns Present at source is: {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn("additional_columns",concat_ws(", ", *extra_columns))\
                .select("customer_id","store_id","product_name","sales_date","sales_person_id","price",
                        "quantity","total_cost","additional_columns")
    else:
        data_df = data_df.withColumn("additional_columns",lit(None))\
                .select("customer_id","store_id","product_name","sales_date","sales_person_id","price",
                        "quantity","total_cost","additional_columns")

    final_df_to_process = final_df_to_process.union(data_df)

logger.info("==================================Final Data Frame from source which will be going to processing===================================")
final_df_to_process.show()

#Enrich the data from all dimension tables
#Create data mart for sales team and calculate the incentive for it
#Another data masrt for the customer and its purchases every month
#For every month there should be a file and there should be a store id segregation
#Read the data from parquet and generate a csv file in which there would be a sales_person_name and and sale_person_store_id
#Sales_person_total_billing_done_for_each_month and total_incentive

#Connecting with DataBase Reader
database_client = DatabaseReader(config.url,config.properties)
#Creating dataframes for all tables
#Customer Table
logger.info("==================================Loading Customer table into customer_table_df===================================")
customer_table_df = database_client.create_dataframe(spark,config.customer_table_name)
logger.info("Customer table df created")
logger.info("==================================Loading Product table into product_table_df===================================")
product_table_df = database_client.create_dataframe(spark,config.product_table)
logger.info("Product table df created")
logger.info("==================================Loading Product staging table into product_staging_table_df===================================")
product_staging_table_df = database_client.create_dataframe(spark,config.product_staging_table)
logger.info("Product Staging table df created")
logger.info("==================================Loading Sales Team table into sales_team_table_df===================================")
sales_team_table_df = database_client.create_dataframe(spark,config.sales_team_table)
logger.info("Sales Team table df created")
logger.info("==================================Loading Store table into store_table_df===================================")
store_table_df = database_client.create_dataframe(spark,config.store_table)
logger.info("Store table df created")

s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,customer_table_df,store_table_df,sales_team_table_df)

#Final Enriched Data
logger.info("==============================Final Enriched Data==============================")
s3_customer_store_sales_df_join.show()

#Writing the customer data to customer data mart in parquet format
#file will be written to local first
#Move the raw data to S3 bucket for BI Team
#Write reporting data to mysql table also
logger.info("=================================Writing the customer data to Customer Data mart df==========================")
final_customer_data_mart_df = s3_customer_store_sales_df_join.select("ct.customer_id",
                                "ct.first_name","ct.last_name","ct.address","ct.pincode","phone_number","sales_date","total_cost")
logger.info("=================================Final data for customer data mart=================================")
final_customer_data_mart_df.show()

parquet_writer = ParquetWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df,config.customer_data_mart_local_file)

logger.info(f"======================Customer Data written to local disk at: {config.customer_data_mart_local_file}===========================")
logger.info("=================================Moving Data from local to S3 for Customer Data Mart========================")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.customer_data_mart_local_file)
logger.info(f"{message}")

#Sales Team Data Mart
logger.info("==============================Writing Sales Team Data to Sales Team Data Mart df=========================")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join.select("store_id","sales_person_id","sales_person_first_name"
                                ,"sales_person_last_name","store_manager_name","manager_id","is_manager","sales_person_address",
                                "sales_person_pincode","sales_date","total_cost",expr("SUBSTRING(sales_date,1,7) as sales_month"))

logger.info("=================================Final data for sales team data mart=================================")
final_sales_team_data_mart_df.show()
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,config.sales_team_data_mart_local_file)
logger.info(f"======================Sales Team Data written to local disk at: {config.sales_team_data_mart_local_file}=========================")
logger.info("=================================Moving Data from local to S3 for Sales Team Data Mart=============================")
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,config.bucket_name,config.sales_team_data_mart_local_file)
logger.info(f"{message}")

#Also Writing the data into partitions
final_sales_team_data_mart_df.write.format("parquet")\
    .option("header","True")\
    .mode("overwrite")\
    .partitionBy("sales_month","store_id")\
    .option("path",config.sales_team_data_mart_partitioned_local_file)\
    .save()

#Move data on s3 into the partitioned folder
s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp())*1000
for root,dirs,files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root,file)
        relative_file_path = os.path.relpath(local_file_path,config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path,config.bucket_name,s3_key)

#Calculation for customer mart
#Find out customer total purchace every month
#Write the data into MYSQL table
logger.info("===================Calculating every month purchaced amount for the customer================")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("=========================Calculation of the customer mart done and written into the table==========================")

#Calculation for sales team mart
#find out the total sales done by each sales person every month
#Give the top performer 1% incentive of total sales of the month
#Rest sales person will get nothing
#Writing the data to MYSQL table.

logger.info("======================Calculating Sales every month billed amount==========================")
sales_mart_sql_transformation_write(final_sales_team_data_mart_df)
logger.info("======================Calculation of sales mart done and written into the table==========================")

#Move the file on S3 into processed folder and delete the local files

source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client,config.bucket_name,source_prefix, destination_prefix)
logger.info(f"{message}")

logger.info("======================Deleting Sales data from Local==========================")
delete_local_file(config.local_directory)
logger.info("======================Deleted Sales data from Local==========================")

logger.info("======================Deleting Customer data mart from Local==========================")
delete_local_file(config.customer_data_mart_local_file)
logger.info("======================Deleted Customer data mart from Local==========================")

logger.info("======================Deleting Sales Team data mart from Local==========================")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("======================Deleted Sales Team data mart from Local==========================")

logger.info("======================Deleting Sales Team partitioned data mart from Local==========================")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("======================Deleted Sales Team partitioned data mart from Local==========================")

#Update the status of staging table
update_statements = []
if corrected_files:
    for file in corrected_files:
        filename = os.path.basename(file)
        statements = f"""
            UPDATE {db_name}.{config.product_staging_table} SET STATUS = 'I', UPDATED_DATE = {formatted_date}
            WHERE FILENAME = {filename}
        """

        update_statements.append(statements)

    logger.info(f"Updated statement created for staging table =====> {update_statements}")
    logger.info("===========Connecting to MYSQL Server==========")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("=============MYSQL connection established================")
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("==================There is some error in the process===============")
    sys.exit()

input("Press enter to terminate")