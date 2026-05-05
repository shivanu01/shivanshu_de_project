

key = "youtube_project"
iv = "youtube_encyptyo"
salt = "youtube_AesEncryption"


#aws details , access and secret key


bucket_name = "feb-de-project"
s3_source_directory = "sales_data/"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"
s3_sales_partitioned_datamart_directory = "sales_partitioned_data_mart/"


#Database credentials
# MYSQL data base connection details

host = "localhost"
database_name = "feb_de_project"
url = f"jdbc:mysql://localhost:3306/{database_name}"

properties = {
    "user" : "root",
    "password" : "Mami@1504",
    "driver" : "com.mysql.cj.jdbc.Driver"

}

#Table name
customer_table = "customer"
product_table = "product"
store_table = "store"
sales_team_table = "sales_team"
product_staging_table = "product_staging_table"

#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

#Mandatory columns

mandatory_column = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]



# File Download location
local_directory = "D:\\FEB_de_Project\\file_from_s3\\"
customer_data_mart_local_file = "D:\\FEB_de_Project\\customer_data_mart\\"
sales_team_data_mart_local_file = "D:\\FEB_de_Project\\sales_team_data_mart\\"
sales_data_mart_partitioned_local_file = "D:\\FEB_de_Project\\sales_partition_data\\"
error_folder_path_local = "D:\\FEB_de_Project\\error_files\\"

