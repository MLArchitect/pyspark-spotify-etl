from utils.logger import get_logger
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

logger = get_logger()

_spark = None


def get_spark(app_name='spotify_data_pipeline'):
    """General function: to create the spark session, can handle both databricks and non-databricks implementations. can support delta files (the builder)

    Args:
        app_name (str, optional): a nice name for the spark application. Defaults to 'spotify_data_pipeline'.

    Returns:
        _spark: returns the spark session
    """
    global _spark
    if _spark is None:
        logger.info('creating a spark session...')
        builder = (
            SparkSession
            .builder
            .appName(app_name)
            .config('spark.sql.adaptive.enabled', 'true')
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        _spark = configure_spark_with_delta_pip(builder).getOrCreate()
        logger.info('spark session created successfully')
    return _spark


def stop_spark():
    global _spark
    if _spark is not None:
        logger.info('stopping the spark session....')
        _spark.stop()
        _spark = None
