from src.main.utility.logging_config import logger
from pyspark.sql.functions import *

def dimension_table_join(final_df_to_process,customer_df,store_df,sales_team_df):
    logger.info("Joining the final df to process with customer table")

    s3_customer_df_join = final_df_to_process.alias('s3')\
                            .join(customer_df.alias("cust"),
                                  col("s3.customer_id")== col('cust.customer_id'),"inner")\
                            .drop("product_name","price","quantity","additional_column",
                                    col("s3.customer_id"),"customer_joining_date")
    logger.info("Joining s3_customer_df_join to store_table_df")

    s3_customer_store_df_join = s3_customer_df_join.\
                                            join(store_df,
                                             store_df["id"]==s3_customer_df_join["store_id"],"inner")\
                                    .drop("id","store_pincode","store_opening_date","reviews")

    logger.info("Joining s3_customer_store_df_join to sales_team_df")
    s3_customer_store_sales_df_join = s3_customer_store_df_join.\
                                        join(sales_team_df.alias("sales"),
                                             s3_customer_store_df_join["sales_person_id"]==col("sales.id")) \
                                        .withColumn("sales_person_first_name", col("sales.first_name")) \
                                        .withColumn("sales_person_last_name", col("sales.last_name")) \
                                        .withColumn("sales_person_address", col("sales.address")) \
                                        .withColumn("sales_person_pincode", col("sales.pincode")) \
                                        .drop("id", col("sales.first_name"), col("sales.last_name"),col("sales.address"),col("sales.pincode"))
    return s3_customer_store_sales_df_join

