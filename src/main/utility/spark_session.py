import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *

class SessionSpark:
    def __init__(self):
        self.session = SparkSession.builder.master("local[*]")\
        .appName("shivanshu-spark")\
        .config("spark.driver.extraClassPath", "C:\\my_sql_jar\\mysql-connector-java-8.0.26.jar") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .getOrCreate()
        logger.info("spark session %s",self.session)

    def spark_session(self):
        return self.session
