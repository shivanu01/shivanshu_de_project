from pyspark.sql import Window
from pyspark.sql.functions import *

from resources.dev import config
from src.main.utility.logging_config import logger
from src.main.write.database_write import DatabaseWriter


def customer_mart_calculation_table_write(final_customer_datamart_df):
    window = Window.partitionBy("customer_id","sales_date_month")
    final_customer_datamart_df = final_customer_datamart_df\
                                    .withColumn("sales_date_month",substring(col("sales_date"),1,7))\
                                    .withColumn("total_sales_every_month_customer_wise",sum(col("total_cost")).over(window))\
                                    .select("customer_id",concat(col("first_name"),lit(" "),col("last_name")).alias("full_name"),
                                            "address","phone_number","sales_date_month",col("total_sales_every_month_customer_wise")
                                            .alias("total_sales")).distinct()
    logger.info("*****************The calculated customer data is******************")
    final_customer_datamart_df.show()

    #Write data into MYSQL customer_data_mart table
    db_writer = DatabaseWriter(url = config.url,properties = config.properties)
    db_writer.write_dataframe(final_customer_datamart_df,config.customer_data_mart_table)

