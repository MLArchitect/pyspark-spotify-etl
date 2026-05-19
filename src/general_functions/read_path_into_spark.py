from utils.logger import get_logger
from pyspark.sql import SparkSession, DataFrame

logger = get_logger()


def read_data(
        spark: SparkSession,
        read_path: str,
        fmt: str = 'delta',
        options: dict | None = None) -> DataFrame:
    """ General function: a function to read the data in any given path with any given format and return a spark dataframe

    Args:
        spark (SparkSession): the spark session, extendible with the given options
        read_path (str): the path to read the data from
        fmt (str, optional): format of the files stored in the given path. Defaults to 'delta'.
        options (dict | None, optional): extend the spark session with any number of options. Defaults to None.

    Returns:
        DataFrame: _description_
    """
    try:
        logger.info(
            f'start reading data at path : {read_path}, formt : {fmt}, options : {options}')
        reader = spark.read
        if options:
            reader = reader.options(**options)

        spark_df = reader.format(fmt).load(read_path)
        logger.info(f'successfully read data at {read_path}')
        return spark_df

    except Exception as e:
        logger.exception(
            f'unexpected error in reading the data at path: {read_path}')
        raise

    finally:
        logger.info('read function call ended')
