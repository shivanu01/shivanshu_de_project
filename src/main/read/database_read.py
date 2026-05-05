class DatabaseReader:
    def __init__(self,url,proerties):
        self.url=url
        self.properties= proerties

    def create_dataframe(self,spark,table_name):
        df = spark.read.jdbc(url = self.url,
                             table=table_name,
                             properties=self.properties)
        return df