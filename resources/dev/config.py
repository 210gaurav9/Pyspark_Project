import os

key = "youtube_project"
iv = "youtube_encyptyo"
salt = "youtube_AesEncryption"

#AWS Access And Secret key
aws_access_key = "94ZIQELpu8yVUnj71k53sW+QTz7UrIvkvVsQ7nDYVr4="
aws_secret_key = "69J6OuYtWW1L/YMK1pkQ2JK24Rcy9nUegjZo6uFXJduRW58yI0bFOAvLt/6grheA"
bucket_name = "pyspark-project-1"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"
"sales_partitioned_data_mart"

#Database credential
# MySQL database connection properties
host = "localhost"
password = "glpJpZveO9dmNG1jkwK3EQ=="
user = "root"
database_name = "pyspark_project"
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]


# File Download location
local_directory = "C:\\Users\\hp\\OneDrive\\Desktop\\Pyspark Project\\Project tables\\file_from_s3\\"
customer_data_mart_local_file = "C:\\Users\\hp\\OneDrive\\Desktop\\Pyspark Project\\Project tables\\customer_data_mart\\"
sales_team_data_mart_local_file = "C:\\Users\\hp\\OneDrive\\Desktop\\Pyspark Project\\Project tables\\sales_team_data_mart\\"
sales_team_data_mart_partitioned_local_file = "C:\\Users\\hp\\OneDrive\\Desktop\\Pyspark Project\\Project tables\\sales_partition_data\\"
error_folder_path_local = "C:\\Users\\hp\\OneDrive\\Desktop\\Pyspark Project\\Project tables\\error_files\\"
