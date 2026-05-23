from pyspark.sql import functions as F
from src.general_functions.spark_manager import get_spark
from src.general_functions.read_path_into_spark import read_data
from src.general_functions.write_into_path import writer
from utils.logger import get_logger
from typing import Dict

spark = get_spark()
logger = get_logger()
DEFAULT_SILVER_WRITE_OPTIONS = {"overwriteSchema": "true"}


def transform_albums(
    read_path: str,
    write_path: str,
    write_mode: str = 'overwrite',
    write_options: Dict[str, str] = DEFAULT_SILVER_WRITE_OPTIONS
) -> None:
    """Transform album data from bronze to silver layer.

    Args:
        read_path: Path to bronze Delta table
        write_path: Path to silver Delta table
        write_mode: Spark write mode (default: 'overwrite')
        write_options: Additional write options
    """
    df = None

    try:
        df = read_data(spark, read_path, 'delta')

        # initial_count

        if not df.head(1):
            raise ValueError(f"no data found at path {read_path}")

    except Exception as e:
        logger.exception(f"failed to read data at path {read_path}")
        raise

    try:
        df.cache()
        initial_count = df.count()
        logger.info(f'read {initial_count} records at path {read_path}')
        try:
            logger.info("starting transformation")

            df_transformed = df.select(
                [F.col('id').alias('album_id'),
                 F.col('name').alias('album_name'),
                 F.col('release_date').cast('date').alias('release_date'),
                 F.col('release_date_precision'),
                 F.col('total_tracks'),
                 F.col('type').alias('object_type'),
                 'album_type',
                 'available_markets',
                 F.col('external_urls.spotify').alias('spotify_url'),
                 'href',
                 'uri'
                 ])\
                .filter(F.col('album_id').isNotNull()).dropDuplicates(['album_id'])\
                .withColumns(
                {
                    'release_year': F.year('release_date'),
                    'release_month': F.month('release_date'),
                    'release_weekday': F.dayofweek('release_date'),
                    'available_markets_count': F.size('available_markets')
                }
            )

            final_count = df_transformed.count()
            if final_count == 0:
                raise ValueError(
                    "all records were filtered out during transformation logic...")

        except ValueError:
            raise  # to catch other valuerror exceptions
        except Exception as e:
            logger.exception(f"failed during transformation")
            raise

        try:
            logger.info(f"writing {final_count} rows to path {write_path}")
            writer(df_transformed, write_path, mode=write_mode,
                   options=write_options)
        except Exception as e:
            logger.exception(f"encountered error: {e}")
            raise
    finally:
        if df:
            df.unpersist()
            logger.debug("unpresisted cached df")
