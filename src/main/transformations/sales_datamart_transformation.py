from pyspark.sql import Window
from pyspark.sql.functions import *

from resources.dev import config
from src.main.utility.logging_config import logger
from src.main.write.database_write import DatabaseWriter


def sales_mart_calculation_table_write(final_sales_team_datamart):
    window = Window.partitionBy("sales_person_id","store_id","sales_month")
    final_sales_team_datamart = final_sales_team_datamart.withColumn("sales_month",substring(col("sales_date"),1,7))\
                             .withColumn("total_sales_by_salesman_every_month",sum("total_cost").over(window))\
                             .select("store_id","sales_person_id",concat(col("sales_person_first_name"),lit(" "),col("sales_person_last_name")).alias("full_name"),
                                            col("sales_month"),col("total_sales_by_salesman_every_month")).distinct()

    rank_window = Window.partitionBy("store_id","sales_month").orderBy(col("total_sales_by_salesman_every_month").desc())

    final_sales_team_datamart = final_sales_team_datamart.withColumn("rnk", rank().over(rank_window))\
                                                    .withColumn("incentive",when(col("rnk")==1,
                                                    col("total_sales_by_salesman_every_month")*0.01).otherwise(lit(0)))\
                                                    .withColumn("incentive",round(col("incentive"),2))\
                                                    .withColumn("total_sales",col("total_sales_by_salesman_every_month"))\
                                                    .select("store_id","sales_person_id","full_name","sales_month",
                                                            "total_sales","incentive")
    logger.info("************Incentive calculated for sales data*********************")
    final_sales_team_datamart.show()
    #Writing data into MYSQL customer data mart table
    db_writer = DatabaseWriter(url=config.url,properties = config.properties)
    db_writer.write_dataframe(final_sales_team_datamart,config.sales_team_data_mart_table)

