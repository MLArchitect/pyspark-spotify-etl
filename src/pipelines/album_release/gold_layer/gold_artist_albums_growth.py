from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.general_functions.read_path_into_spark import read_data
from src.general_functions.spark_manager import get_spark
from src.general_functions.upsert_into_path import upsert
from utils.logger import get_logger
from datetime import datetime
from typing import Dict


spark = get_spark()
logger = get_logger(__name__)


def create_artist_album_growth(base_silver_path: str,
                               fact_albums_path: str,
                               bridge_album_artist_path: str,
                               dim_artists_path: str,
                               gold_path: str,
                               gold_artist_growth_path: str,
                               upsert_primary_key_cols: list[str],
                               partition_by_cols: list[str] = None
                               ) -> Dict:
    """
    Calculate year-over-year album growth metrics for artists.

    Creates a gold layer table with:
    - YoY growth rates
    - Rolling averages (3-year window)
    - Cumulative album counts
    - Release gap analysis

    Args:
        base_silver_path: Path to silver layer
        fact_albums_path: Relative path to fact_albums table
        bridge_album_artist_path: Relative path to bridge table
        dim_artists_path: Relative path to dim_artists table
        gold_path: Base path for gold layer
        gold_artist_growth_path: Relative path for output table
        upsert_primary_key_cols: Columns for merge condition
        partition_by_cols: Optional partitioning columns

    Returns:
        dict: Performance metrics including stage timings and throughput
    """
    metrics = {'stages': {}}
    pipeline_start = datetime.now()

    try:
        stage_start = datetime.now()
        # join dim and fact artists and albums -- base df
        fact_df = read_data(spark, f'{base_silver_path}/{fact_albums_path}')
        bridge_df = read_data(
            spark, f'{base_silver_path}/{bridge_album_artist_path}')
        dim_artist = read_data(spark, f'{base_silver_path}/{dim_artists_path}')
        logger.info('all dataframes read successfully')
        metrics['stages']['read'] = {
            'duration_seconds': (datetime.now() - stage_start).total_seconds(),
            'tables_read': 3
        }
    except Exception as e:
        logger.exception(f'problem in reading the tables: {e}')
        raise

    try:
        # count number of tracks per artist in each year
        stage_start = datetime.now()
        joined_df = fact_df\
            .join(bridge_df, on='album_id')\
            .join(dim_artist, on='artist_id')\
            .groupBy(['artist_id', 'artist_name', 'release_year']).agg(F.count('album_id').alias('num_albums'))
        # window function
        window_spec = Window.partitionBy('artist_id').orderBy('release_year')

        # growth metrics columns
        growth_columns = {
            'previous_year_num_albums': F.lag('num_albums').over(window_spec),
            'growth_album_release': F.when(
                F.lag('num_albums').over(window_spec) > 0,
                (F.col('num_albums') - F.lag('num_albums').over(window_spec)) *
                100 / F.lag('num_albums').over(window_spec)
            ),
            'years_since_last_release': F.coalesce(
                F.col('release_year') -
                F.lag('release_year').over(window_spec),
                F.lit(0)
            )
        }

        # aggregation metric columns
        agg_columns = {
            'cumulative_albums': F.sum('num_albums').over(window_spec),
            'rolling_avg_3y': F.avg('num_albums').over(window_spec.rowsBetween(-2, 0)),
            'rolling_max_3y': F.max('num_albums').over(window_spec.rowsBetween(-2, 0))
        }

        result_df = joined_df.withColumns(
            growth_columns).withColumns(agg_columns)

        result_count = result_df.count()
        metrics['stages']['process'] = {
            'duration_seconds': (datetime.now() - stage_start).total_seconds(),
            'rows_processed': result_count}
        logger.info('aggregation logic applied successfully')

    except Exception as e:
        logger.exception(f'encountered error in the aggregation part: {e}')
        raise

    # write into path
    try:
        stage_start = datetime.now()
        upsert_result = upsert(result_df,
                               f'{gold_path}/{gold_artist_growth_path}',
                               upsert_primary_key_cols,
                               partition_by=partition_by_cols,
                               enable_schema_evolution=True)
        metrics['stages']['write'] = {
            'duration_seconds': (datetime.now() - stage_start).total_seconds(),
            'rows_written': upsert_result.get('rows_inserted', 0) + upsert_result.get('rows_updated', 0)}

        total_duration = (datetime.now() - pipeline_start).total_seconds()
        metrics['summary'] = {
            'total_duration_seconds': total_duration,
            'throughput_rows_per_second': result_count / total_duration if total_duration > 0 else 0,
            'completed_at': datetime.now().isoformat()}
        logger.info(f"""
                    Pipeline Performance:
                    - Total: {total_duration:.1f}s
                    - Read: {metrics['stages']['read']['duration_seconds']:.1f}s
                    - Process: {metrics['stages']['process']['duration_seconds']:.1f}s ({result_count} rows)
                    - Write: {metrics['stages']['write']['duration_seconds']:.1f}s
                    - Throughput: {metrics['summary']['throughput_rows_per_second']:.0f} rows/sec
                    """)

        return metrics

    except Exception as e:
        logger.exception(
            f'encountered an error when writing the final results: {e}')
        raise
