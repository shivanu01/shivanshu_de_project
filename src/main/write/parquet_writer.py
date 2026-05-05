from src.main.utility.logging_config import logger


class ParquetWriter:
    def __init__(self, mode, data_format):
        self.mode = mode
        self.data_format = data_format

    def dataframe_writer(self, df, file_path):
        try:
            df.write \
              .format(self.data_format) \
              .mode(self.mode) \
              .save(file_path)

        except Exception as e:
            logger.error(f"Error writing the data : {str(e)}")
            raise e
