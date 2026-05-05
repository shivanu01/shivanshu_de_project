

#firstly we will connect to s3
#we will get an s3 client object to perform the operations
import datetime
import os
import shutil
import sys
import time

import findspark
from dotenv import load_dotenv

from src.main.delete.local_file_delete import delete_local_file

findspark.init()


from src.main.transformations.customer_data_mart_sql_transformation import customer_mart_calculation_table_write
from src.main.transformations.sales_datamart_transformation import sales_mart_calculation_table_write
from src.main.upload.s3_upload import UploadToS3
from src.main.write.parquet_writer import ParquetWriter



from resources.dev import config
from src.main.download.aws_download import S3FileDownloader
from src.main.move.move_files import moveS3toS3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.dimension_table_join import dimension_table_join
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import MySQLConnectionProvider
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.spark_session import SessionSpark


from pyspark.sql.functions import concat_ws, lit,expr
from pyspark.sql.types import StructType, StringType, StructField, FloatType, IntegerType, DateType

load_dotenv()

aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")

#getting s3 client using S3ClientProvider class which is a wrapper class for boto
s3_client_provider = S3ClientProvider(decrypt(aws_access_key),decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()

# print(response['Buckets'])
# for key in response['Buckets']:
#     print(key)

logger.info(f"List of buckets are {response['Buckets']}")

#check if the local directory already has file
# if file is present then check if the same file is present in the staging area with status ='A'
# if so then don't delete and try to re run,else give an error and not proceed to next file

csv_files  = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]


sql_connection_provider = MySQLConnectionProvider()
connection = sql_connection_provider.get_mysql_connection()
cursor = connection.cursor()

if csv_files:

    statement = f"""select distinct file_name from {config.database_name}.{config.product_staging_table}
                         where file_name in ({str(csv_files)[1:-1]}) and status ='A' """

    logger.info(f"Dynamically statement created : {statement}")

    cursor.execute(statement)
    data = cursor.fetchall()

    if data:
        logger.info("your last run failed, please check")
        raise Exception("Stopping the program because the previous run failed")

    else:
        logger.info("No record matches")

    cursor.close()
    connection.close()


else:
    logger.info("Last run was successfull")



# now we will read the files from s3 which are present in sales/data folder
#getting absolute file path for the file present in s3
try :
    s3_reader = S3Reader()
    s3_absolute_file_path = s3_reader.list_files(s3_client,config.bucket_name,config.s3_source_directory)
    logger.info(f"Absolute file path for the files in s3 bucket is {s3_absolute_file_path}")
    if not s3_absolute_file_path:
        logger.info(f"No files present in {config.s3_source_directory}")
        raise Exception("no data available to process")

except Exception as e:
    logger.info("Exited with error %s",e)
    raise e


bucket_name = config.bucket_name
local_directory = config.local_directory

# Now we will download these file from s3

prefix = f"s3://{bucket_name}/"
file_paths = [ url[len(prefix):] for url in s3_absolute_file_path]
logger.info(f"File path avaiable in s3 under bucket name {bucket_name} and folder name {file_paths}")

try:
    downloader = S3FileDownloader(s3_client,bucket_name,local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error(f"file download error {e}")
    sys.exit()

#GET ALL the csv files and store error file seperately

all_files  = os.listdir(local_directory)

if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        # print(files)
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory,files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory,files)))
    if not csv_files:
        logger.info("No csv files present to process")
        raise Exception("No data to process")
else:
    logger.info("There are no files to process")
    raise Exception("There is no data to process")


logger.info("Creating a spark session")
sessionGetter = SessionSpark()
spark = sessionGetter.spark_session()
logger.info("Spark session created")



correct_files = []

for data in csv_files:
    print(data)
    data_schema = spark.read.format("csv")\
                        .option("header","true")\
                        .load(data).columns
    logger.info(f"schema of {data} is {data_schema}")
    logger.info(f"mandatory columns are {config.mandatory_column}")
    missing_columns = set(config.mandatory_column)-set(data_schema)
    logger.info(f"missing  columns are {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"there are no missing columns present in {data}")
        correct_files.append(data)

logger.info(f"List of correct files {correct_files}")
logger.info(f"List of error files {error_files}")


logger.info("Moving the error files to the local directory if any are present")

#
# move the error files to local error directory
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(config.error_folder_path_local,file_name)
            shutil.move(file_path,destination_path)
            logger.info(f"Moved file '{file_name}' from s3 file path to '{destination_path}'")

            # now we will have to move the same file in error directory in s3 as well
            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = moveS3toS3(s3_client,config.bucket_name,source_prefix,destination_prefix,file_name)
            logger.info(f"{message}")
        else:
            logger.info(f"File path {file_path} does not exists")
else:
    logger.info("There is no error file present in the directory")



# Before running the process,update the staging table with Status (A) a active and I as Inactive


logger.info("Updating the product staging table that we have started")

insert_statement = []

db_name = config.database_name
current_date  =datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")

if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)
        statement = f"Insert into {db_name}.{config.product_staging_table}" \
                     f"(file_name,file_location,created_date,status)" \
                     f"Values ('{file_name}','{file}','{formatted_date}','A')"
        insert_statement.append(statement)
    logger.info(f"Insert statment created for staging table {insert_statement}")

    logger.info("*********Connectiong with MYSQL***********")

    sql_connection_provider1 = MySQLConnectionProvider()
    connection = sql_connection_provider1.get_mysql_connection()
    cursor = connection.cursor()

    logger.info("**********Mysql connected successfully*****************")

    for statement in insert_statement:
        cursor.execute(statement)
    connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("There are no files to process")
    raise Exception("*******There are no files that can be processed**********")

logger.info("Staging table created successfully")



# Additional column needs to be taken care of, determine the extra columns

logger.info("Fixing extra columns from the source")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),  # fixed
    StructField("additional_column", StringType(), True)  # fixed
])

final_df_to_process = spark.createDataFrame([],schema=schema)

# Create a new column with concated value of extra column

for data in correct_files:
    data_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferschema", "true") \
        .load(data)
    data_schema = data_df.columns
    extra_columns = set(data_schema) - set(config.mandatory_column)
    logger.info(f"Extra columns present are  {extra_columns}")

    if extra_columns:
        data_df.select(*extra_columns).show()

        data_df = data_df.withColumn("additional_column", concat_ws(",", *extra_columns))\
                         .select("customer_id","store_id","product_name","sales_date","sales_person_id",
                                 "price","quantity","total_cost","additional_column")
        logger.info(f"Process data {data} and added 'additional column'")
    else:
        data_df = data_df.withColumn("additional_column",lit(None))\
                         .select("customer_id","store_id","product_name","sales_date","sales_person_id",
                                 "price","quantity","total_cost","additional_column")
        logger.info(f"Processed data {data} and added 'None' in additional column")
    final_df_to_process = final_df_to_process.union(data_df)

logger.info("***********Final dataframe which will be used to process is *************")
final_df_to_process.show()

#Enrich the data from all dimension tables

# create a datamart for sales team and their incentive,adress and other details
#create another datamart for customers who bought how many items each days of a month

#for every month there should be a file
# and inside that file there should be a store id for segregation

##*********Read the data from parquet and generate a csv file in which there will be a sales_person_name ,sales_person_store_id
# sales_person_total_billing_done _for_each_month, total_incentive

#connecting to database reader

database_client = DatabaseReader(config.url,config.properties)

#Creating dataframe for all table

#Customer dataframe

logger.info("*****Loading customer table into customer_table_df*******")
customer_table_df = database_client.create_dataframe(spark,config.customer_table)


logger.info("*****Loading product table into product_table_df*******")
product_table_df = database_client.create_dataframe(spark,config.product_table)

logger.info("*****Loading store table into store_table_df*******")
store_table_df = database_client.create_dataframe(spark,config.store_table)

logger.info("**********Loading sales team table into sales_team_table_df******************")
sales_team_table_df = database_client.create_dataframe(spark,config.sales_team_table)

s3_customer_sales_store_df_join = dimension_table_join(final_df_to_process,customer_table_df,store_table_df,sales_team_table_df)


logger.info("Final enriched data")
s3_customer_sales_store_df_join.show()


#Write customer data to customer_Data_mart in parquet form
# file will be written to local first
# move the raw data to s3 bucket for reporting tool
# write reporting data into MYSQL table also

final_customer_data_mart_df = s3_customer_sales_store_df_join\
                                .select("cust.customer_id","cust.first_name","cust.last_name","cust.address",
                                        "cust.pincode","cust.phone_number","sales_date","total_cost")
logger.info("****Customer datamart***")
final_customer_data_mart_df.show()

#writing data to parquet format

parquet_writer = ParquetWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df,config.customer_data_mart_local_file)

logger.info(f"******Customer_data_mart written to local disk at {config.customer_data_mart_local_file}*******")

# Now we will upload the data to s3

s3_uploader= UploadToS3(s3_client)
message = s3_uploader.upload_to_s3(config.s3_customer_datamart_directory,config.bucket_name,config.customer_data_mart_local_file)
logger.info(f"{message}")
logger.info("customer_data_mart uploded successfully to s3")


#sales team data mart
final_sales_team_datamart = s3_customer_sales_store_df_join.select("store_id","sales_person_id","sales_person_first_name"
                                            ,"sales_person_last_name","store_manager_name","manager_id","is_manager",
                                            "sales_person_address","sales_person_pincode","sales_date"
                                            ,"total_cost",expr("SUBSTRING(sales_date,1,7) as sales_month"))
logger.info("Final data for sales team data mart")
final_sales_team_datamart.show()

parquet_writer.dataframe_writer(final_sales_team_datamart,config.sales_team_data_mart_local_file)
logger.info(f"Sales team data mart written in disk at {config.sales_team_data_mart_local_file}")

#upload to s3
message = s3_uploader.upload_to_s3(config.s3_sales_datamart_directory,config.bucket_name,config.sales_team_data_mart_local_file)
logger.info(f"{message}")
logger.info("customer_data_mart uploded successfully to s3")

#Writing data into partition
logger.info("Partitioning sales data")
final_sales_team_datamart.write.format("parquet")\
    .option("header","true")\
    .mode("overwrite")\
    .partitionBy("sales_month","store_id")\
    .option("path",config.sales_data_mart_partitioned_local_file)\
    .save()

logger.info("sales data partitioned")


logger.info("Moving partitioned data to s3")
#move partitioned data to s3
s3_prefix =config.s3_sales_partitioned_datamart_directory
current_epoch = int(datetime.datetime.now().timestamp())*1000
for root,dir,files in os.walk(config.sales_data_mart_partitioned_local_file):
    for file in files:
        local_file_path = os.path.join(root,file)
        relative_file_path = os.path.relpath(local_file_path,config.sales_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path,config.bucket_name,s3_key)
logger.info("data moved to s3 successfully")


#Calculation for customer mart
#find out the customer total purchase every month
#write the data into my SQL table
# future query- : we can give customer with highest sale coupons

logger.info("Calculating customer every month purchased amount")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("Calculation of customer mart done and written to table")

# calculation for sales team mart
# find out the total sales done by each sales person in each month
# give the top performer 1% incentive of total sales of the month
# rest sales person will get nothing
# write the data into msql table
logger.info("Calculating data for sales team mart")
sales_mart_calculation_table_write(final_sales_team_datamart)
logger.info("Calculation done for sales team mart")


#Move the  file on s3 to processed folder and delete the local files
source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = moveS3toS3(s3_client,config.bucket_name,source_prefix, destination_prefix)
logger.info(f"{message}")


logger.info("deleting sales data from local")
delete_local_file(config.local_directory)
logger.info("sales data deleted from local")


logger.info("deleting customer data mart from local")
delete_local_file(config.customer_data_mart_local_file)
logger.info("Customer data mart deleted successfully")

logger.info("deleting sales data mart from local")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("sales data mart deleted successfully")

logger.info("deleting sales partitioned from local")
delete_local_file(config.sales_data_mart_partitioned_local_file)
logger.info("partitioned data deleted from local")

update_statement = []

if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)

        statement = f"UPDATE {db_name}.{config.product_staging_table} " \
                    f"SET status='I', updated_date='{formatted_date}' " \
                    f"WHERE file_name='{file_name}'"

        update_statement.append(statement)

    logger.info(f"updated statements created for staging table --- {update_statement}")
    logger.info("**********connecting with MYSQL server**********")

    sql_connection_provider = MySQLConnectionProvider()
    connection = sql_connection_provider.get_mysql_connection()
    cursor = connection.cursor()

    logger.info("My sql server connected successfully")

    for statement in update_statement:
        cursor.execute(statement)

    connection.commit()

    cursor.close()
    connection.close()

else:
    logger.error("******There is some error in process in between*******")
    sys.exit()

input("Press enter to terminate")
